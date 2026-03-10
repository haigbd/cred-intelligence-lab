#!/usr/bin/env python3
"""
Commercial DB Audit Script
===========================
Audits a BigQuery project for CRM data exposure, enrichment coverage,
unique data points, and generates an HTML report.

Usage:
    python scripts/commercial_db_audit.py --project cred-commercial
    python scripts/commercial_db_audit.py --project cred-commercial --billing-project cred-1556636033881
    python scripts/commercial_db_audit.py --project cred-commercial --output /tmp/report.html
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore", message=".*BigQuery Storage module not found.*")

import google.auth
from google.cloud import bigquery
import pandas as pd


# ─── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Audit a commercial BigQuery project")
    p.add_argument("--project", required=True, help="BigQuery project ID to audit (e.g. cred-commercial)")
    p.add_argument("--billing-project", default="cred-1556636033881",
                    help="Project that runs queries and has cred_seed (default: cred-1556636033881)")
    p.add_argument("--schemas", nargs="+",
                    default=["commercial_model_prod", "commercial_credentity", "cred_enrichment"],
                    help="Key schemas to focus on")
    p.add_argument("--output", default=None, help="Output HTML path (default: reports/output/<project>_audit_<date>.html)")
    return p.parse_args()


# ─── BigQuery helpers ──────────────────────────────────────────────────────────

def connect(billing_project: str) -> bigquery.Client:
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials, _ = google.auth.default(scopes=scopes)
    if hasattr(credentials, "with_scopes") and not credentials.scoped:
        credentials = credentials.with_scopes(scopes)
    return bigquery.Client(project=billing_project, credentials=credentials)


def run_query(bq: bigquery.Client, sql: str) -> pd.DataFrame:
    return bq.query(sql).to_dataframe()


def safe_query(bq: bigquery.Client, sql: str, section: str) -> pd.DataFrame | None:
    """Run a query, returning None on error instead of crashing."""
    try:
        return run_query(bq, sql)
    except Exception as e:
        print(f"  ⚠  {section}: {e}")
        return None


# ─── Analysis sections ─────────────────────────────────────────────────────────

def section_schemas(bq, proj):
    print("[1/11] Schema inventory...")
    df = safe_query(bq, f"""
        SELECT schema_name FROM `{proj}`.INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name
    """, "schemas")
    return {"schemas": df["schema_name"].tolist() if df is not None else []}


def section_columns(bq, proj, key_schemas):
    print("[2/11] Column inventory...")
    results = {}
    all_cols = pd.DataFrame()
    for schema in key_schemas:
        df = safe_query(bq, f"""
            SELECT table_name, column_name, data_type
            FROM `{proj}`.{schema}.INFORMATION_SCHEMA.COLUMNS
            ORDER BY table_name, ordinal_position
        """, f"columns/{schema}")
        if df is not None:
            df["schema"] = schema
            all_cols = pd.concat([all_cols, df], ignore_index=True)
            results[schema] = {"tables": int(df["table_name"].nunique()), "columns": len(df)}
            print(f"       {schema}: {results[schema]['tables']} tables, {results[schema]['columns']} columns")
        else:
            results[schema] = {"tables": 0, "columns": 0}

    total_tables = all_cols["table_name"].nunique() if len(all_cols) > 0 else 0
    total_cols = len(all_cols)
    print(f"       Total: {total_tables} tables, {total_cols} columns")
    return {"schema_stats": results, "total_tables": total_tables, "total_columns": total_cols}


def section_table_volumes(bq, proj):
    print("[3/11] Table volumes...")
    df = safe_query(bq, f"""
        SELECT table_schema, table_name,
            total_rows AS row_count,
            ROUND(total_logical_bytes / 1024 / 1024, 2) AS size_mb,
            creation_time AS created_at
        FROM `{proj}`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
        ORDER BY total_rows DESC
    """, "table_volumes")
    if df is None:
        return {"active_tables": 0, "empty_tables": 0, "total_rows": 0, "total_storage_mb": 0, "top_tables": []}

    active = df[df["row_count"] > 0]
    return {
        "active_tables": len(active),
        "empty_tables": len(df) - len(active),
        "total_rows": int(active["row_count"].sum()),
        "total_storage_mb": float(active["size_mb"].sum()),
        "top_tables": active.nlargest(30, "row_count")[["table_schema", "table_name", "row_count", "size_mb"]].to_dict("records"),
    }


def section_crm_plans(bq, billing_proj):
    print("[4/11] CRM plan analysis...")
    crm_tagged = safe_query(bq, f"""
        SELECT id, name, description,
            UPPER(crmPlan) AS crmPlan,
            UPPER(insightsPlan) AS insightsPlan,
            UPPER(enrichmentPlan) AS enrichmentPlan,
            uiType, isVisible
        FROM `{billing_proj}`.cred_seed.DataDescriptionMetadata
        WHERE crmPlan IS NOT NULL AND TRIM(crmPlan) != ''
        ORDER BY crmPlan, name
    """, "crm_plans")

    expansion = safe_query(bq, f"""
        SELECT id, name, description,
            UPPER(insightsPlan) AS insightsPlan,
            UPPER(enrichmentPlan) AS enrichmentPlan,
            uiType
        FROM `{billing_proj}`.cred_seed.DataDescriptionMetadata
        WHERE (insightsPlan IS NOT NULL OR enrichmentPlan IS NOT NULL)
          AND (crmPlan IS NULL OR TRIM(crmPlan) = '')
        ORDER BY insightsPlan, name
    """, "crm_expansion")

    cross = safe_query(bq, f"""
        SELECT
            UPPER(COALESCE(crmPlan, '')) AS crmPlan,
            UPPER(COALESCE(insightsPlan, '')) AS insightsPlan,
            UPPER(COALESCE(enrichmentPlan, '')) AS enrichmentPlan,
            COUNT(*) AS count
        FROM `{billing_proj}`.cred_seed.DataDescriptionMetadata
        WHERE crmPlan IS NOT NULL OR insightsPlan IS NOT NULL OR enrichmentPlan IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY count DESC
    """, "cross_plan")

    insights_count = 0
    enrichment_count = 0
    if cross is not None:
        insights_count = int(cross[cross["insightsPlan"] != ""]["count"].sum())
        enrichment_count = int(cross[cross["enrichmentPlan"] != ""]["count"].sum())

    crm_records = crm_tagged.to_dict("records") if crm_tagged is not None else []
    expansion_records = expansion.to_dict("records") if expansion is not None else []

    # Assign suggested CRM tiers based on lowest available tier
    tier_order = {"BASE": 0, "PRO": 1, "CUSTOM": 2}
    for rec in expansion_records:
        tiers = []
        for plan_key in ["insightsPlan", "enrichmentPlan"]:
            val = rec.get(plan_key)
            if val and val in tier_order:
                tiers.append(val)
        if tiers:
            rec["suggestedCrmTier"] = min(tiers, key=lambda t: tier_order[t])
        else:
            rec["suggestedCrmTier"] = "CUSTOM"

    print(f"       CRM-tagged: {len(crm_records)}, Expansion candidates: {len(expansion_records)}")
    return {
        "crm_tagged": crm_records,
        "expansion": expansion_records,
        "insights_count": insights_count,
        "enrichment_count": enrichment_count,
    }


def section_core_tables(bq, proj):
    print("[5/11] Core CRM tables...")
    account = safe_query(bq, f"""
        SELECT COUNT(*) AS total,
            COUNTIF(companyId IS NOT NULL) AS has_company_id,
            ROUND(COUNTIF(companyId IS NOT NULL) / COUNT(*) * 100, 1) AS company_id_pct,
            COUNTIF(website IS NOT NULL) AS has_website,
            ROUND(COUNTIF(website IS NOT NULL) / COUNT(*) * 100, 1) AS website_pct
        FROM `{proj}`.commercial_model_prod.Account
    """, "account")

    contact = safe_query(bq, f"""
        SELECT COUNT(*) AS total,
            COUNTIF(personId IS NOT NULL) AS has_person_id,
            ROUND(COUNTIF(personId IS NOT NULL) / COUNT(*) * 100, 1) AS person_id_pct,
            COUNTIF(email IS NOT NULL) AS has_email,
            ROUND(COUNTIF(email IS NOT NULL) / COUNT(*) * 100, 1) AS email_pct
        FROM `{proj}`.commercial_model_prod.Contact
    """, "contact")

    activity = safe_query(bq, f"""
        SELECT COUNT(*) AS total,
            COUNTIF(createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS last_7d,
            COUNTIF(createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS last_30d,
            MIN(createdAt) AS earliest, MAX(createdAt) AS latest
        FROM `{proj}`.commercial_model_prod.Activity
    """, "activity")

    def first_row(df):
        if df is not None and len(df) > 0:
            row = df.iloc[0].to_dict()
            for k, v in row.items():
                if isinstance(v, (pd.Timestamp,)):
                    row[k] = str(v)
                elif pd.isna(v):
                    row[k] = None
            return row
        return {}

    return {"account": first_row(account), "contact": first_row(contact), "activity": first_row(activity)}


def section_events_collections(bq, proj):
    print("[6/11] Events & collections...")
    events = safe_query(bq, f"""
        SELECT eventType, COUNT(*) AS cnt
        FROM `{proj}`.commercial_model_prod.UserEvent
        GROUP BY eventType ORDER BY cnt DESC
    """, "events")

    collections = safe_query(bq, f"""
        SELECT
            (SELECT COUNT(*) FROM `{proj}`.commercial_model_prod.Collection) AS total_collections,
            (SELECT COUNT(DISTINCT userId) FROM `{proj}`.commercial_model_prod.Collection) AS unique_users,
            (SELECT COUNT(*) FROM `{proj}`.commercial_model_prod.CollectionItem) AS total_items
    """, "collections")

    return {
        "events": events.to_dict("records") if events is not None else [],
        "events_total": int(events["cnt"].sum()) if events is not None else 0,
        "collections": collections.iloc[0].to_dict() if collections is not None and len(collections) > 0 else {},
    }


def section_unipile(bq, proj):
    print("[7/11] Unipile/LinkedIn...")
    df = safe_query(bq, f"""
        SELECT provider, COUNT(*) AS cnt
        FROM `{proj}`.commercial_model_prod.UserUnipileConnection
        GROUP BY provider ORDER BY cnt DESC
    """, "unipile")
    if df is None:
        return {"status": "not_found", "total": 0}
    total = int(df["cnt"].sum())
    status = "test_only" if total < 10 else "active"
    print(f"       Unipile: {total} connections ({status})")
    return {"status": status, "total": total, "providers": df.to_dict("records")}


def section_person_enrichment(bq, proj):
    print("[8/11] Person enrichment...")
    sources = safe_query(bq, f"""
        SELECT source, COUNT(*) AS cnt
        FROM `{proj}`.cred_enrichment.PersonEnrichmentInput
        GROUP BY source ORDER BY cnt DESC
    """, "person_sources")

    coverage = safe_query(bq, f"""
        SELECT COUNT(*) AS total,
            ROUND(COUNTIF(age IS NOT NULL) / COUNT(*) * 100, 1) AS age_pct,
            ROUND(COUNTIF(gender IS NOT NULL) / COUNT(*) * 100, 1) AS gender_pct,
            ROUND(COUNTIF(estimatedSalary IS NOT NULL) / COUNT(*) * 100, 1) AS salary_pct,
            ROUND(COUNTIF(consumerSpendIndex IS NOT NULL) / COUNT(*) * 100, 1) AS consumerSpend_pct,
            ROUND(COUNTIF(educationLevel IS NOT NULL) / COUNT(*) * 100, 1) AS education_pct,
            ROUND(COUNTIF(maritalStatus IS NOT NULL) / COUNT(*) * 100, 1) AS marital_pct,
            ROUND(COUNTIF(numberOfChildren IS NOT NULL) / COUNT(*) * 100, 1) AS children_pct,
            ROUND(COUNTIF(interests IS NOT NULL) / COUNT(*) * 100, 1) AS interests_pct,
            ROUND(COUNTIF(linkedInProfileUrl IS NOT NULL) / COUNT(*) * 100, 1) AS linkedin_pct,
            ROUND(COUNTIF(seniority IS NOT NULL) / COUNT(*) * 100, 1) AS seniority_pct
        FROM `{proj}`.cred_enrichment.PersonEnrichment
    """, "person_coverage")

    total = 0
    cov_data = {}
    if coverage is not None and len(coverage) > 0:
        total = int(coverage["total"].iloc[0])
        cov_data = {k.replace("_pct", ""): float(v) for k, v in coverage.drop(columns="total").iloc[0].items()}

    return {
        "sources": sources.to_dict("records") if sources is not None else [],
        "total_records": total,
        "coverage": cov_data,
    }


def section_company_enrichment(bq, proj):
    print("[9/11] Company enrichment...")
    sources = safe_query(bq, f"""
        SELECT source, COUNT(*) AS cnt
        FROM `{proj}`.cred_enrichment.CompanyEnrichmentInput
        GROUP BY source ORDER BY cnt DESC
    """, "company_sources")

    coverage = safe_query(bq, f"""
        SELECT COUNT(*) AS total,
            ROUND(COUNTIF(name IS NOT NULL) / COUNT(*) * 100, 1) AS name_pct,
            ROUND(COUNTIF(financialHealthScore IS NOT NULL) / COUNT(*) * 100, 1) AS financialHealth_pct,
            ROUND(COUNTIF(averageDemographicFit IS NOT NULL) / COUNT(*) * 100, 1) AS demographicFit_pct,
            ROUND(COUNTIF(sponsorshipDealsCount IS NOT NULL) / COUNT(*) * 100, 1) AS sponsorshipDeals_pct,
            ROUND(COUNTIF(numberOfEmployees IS NOT NULL) / COUNT(*) * 100, 1) AS employees_pct,
            ROUND(COUNTIF(salesTeamSize IS NOT NULL) / COUNT(*) * 100, 1) AS salesTeam_pct,
            ROUND(COUNTIF(marketingTeamSize IS NOT NULL) / COUNT(*) * 100, 1) AS marketingTeam_pct,
            ROUND(COUNTIF(salesTeamProportion IS NOT NULL) / COUNT(*) * 100, 1) AS salesProportion_pct,
            ROUND(COUNTIF(marketingTeamProportion IS NOT NULL) / COUNT(*) * 100, 1) AS marketingProportion_pct,
            ROUND(COUNTIF(hasExecutiveGolfInterest IS NOT NULL) / COUNT(*) * 100, 1) AS execGolf_pct
        FROM `{proj}`.cred_enrichment.CompanyEnrichment
    """, "company_coverage")

    total = 0
    cov_data = {}
    if coverage is not None and len(coverage) > 0:
        total = int(coverage["total"].iloc[0])
        cov_data = {k.replace("_pct", ""): float(v) for k, v in coverage.drop(columns="total").iloc[0].items()}

    return {
        "sources": sources.to_dict("records") if sources is not None else [],
        "total_records": total,
        "coverage": cov_data,
    }


def section_data_sources(bq, billing_proj):
    print("[10/11] Data sources...")
    df = safe_query(bq, f"""
        SELECT name, type, description
        FROM `{billing_proj}`.cred_seed.DataSources ORDER BY name
    """, "data_sources")
    return {"count": len(df) if df is not None else 0, "sources": (df.to_dict("records") if df is not None else [])}


def section_unique_data():
    print("[11/11] Unique data points...")
    person = [
        ("Demographics", ["age", "gender", "maritalStatus", "numberOfChildren", "estimatedSalary"]),
        ("Consumer Spend", ["consumerSpendIndex", "consumerSpendCategory"]),
        ("Interests", ["interests", "followedBrands"]),
        ("Professional", ["educationLevel", "seniority"]),
        ("Social", ["linkedInProfileUrl", "linkedInConnections"]),
    ]
    company = [
        ("Team Composition", ["salesTeamSize", "marketingTeamSize", "salesTeamProportion", "marketingTeamProportion"]),
        ("Scores", ["financialHealthScore", "averageDemographicFit", "averageAudienceAge"]),
        ("Sponsorship", ["sponsorshipDealsCount", "lastSponsoredDate", "sponsorUnitedDealId"]),
        ("Intelligence", ["webTraffic12Months", "numberOfEmployees", "fundingRaised"]),
        ("Signals", ["hasExecutiveGolfInterest", "averageAudienceSalary"]),
        ("Social", ["linkedInOrgUrn", "linkedInData"]),
    ]
    return {
        "person": [{"category": c, "fields": f, "count": len(f)} for c, f in person],
        "company": [{"category": c, "fields": f, "count": len(f)} for c, f in company],
        "person_total": sum(len(f) for _, f in person),
        "company_total": sum(len(f) for _, f in company),
    }


# ─── HTML Report Generation ───────────────────────────────────────────────────

def fmt(n: int | float) -> str:
    """Format a number with commas."""
    if isinstance(n, float):
        return f"{n:,.1f}"
    return f"{n:,}"


def generate_report(data: dict, output_path: str):
    proj = data["project"]
    today = data["date"]
    d = data

    # Helpers for CRM tier table
    def tier_table_rows(items, status_label, status_color, status_weight):
        rows = ""
        for item in items:
            ins = item.get("insightsPlan") or "&mdash;"
            enr = item.get("enrichmentPlan") or "&mdash;"
            ui = item.get("uiType") or "&mdash;"
            if ui == "None":
                ui = "&mdash;"
            rows += f'<tr><td class="name">{item["name"]}</td><td>{ui}</td><td>{ins}</td><td>{enr}</td>'
            rows += f'<td><span style="color:{status_color};font-weight:{status_weight}">{status_label}</span></td></tr>\n'
        return rows

    # Group expansion candidates by suggested tier
    base_exp = [r for r in d["crm_plans"]["expansion"] if r.get("suggestedCrmTier") == "BASE"]
    pro_exp = [r for r in d["crm_plans"]["expansion"] if r.get("suggestedCrmTier") == "PRO"]
    custom_exp = [r for r in d["crm_plans"]["expansion"] if r.get("suggestedCrmTier") == "CUSTOM"]

    # Existing CRM items by tier
    base_existing = [r for r in d["crm_plans"]["crm_tagged"] if r.get("crmPlan") == "BASE"]
    custom_existing = [r for r in d["crm_plans"]["crm_tagged"] if r.get("crmPlan") == "CUSTOM"]

    base_total = len(base_existing) + len(base_exp)
    pro_total = len(pro_exp)
    custom_total = len(custom_existing) + len(custom_exp)

    # Person/company enrichment chart data
    pe = d["person_enrichment"]["coverage"]
    pe_labels = json.dumps([k.replace("consumerSpend", "Consumer Spend").replace("linkedin", "LinkedIn")
                            .replace("marital", "Marital Status").replace("children", "# Children")
                            .replace("salary", "Salary").replace("seniority", "Seniority")
                            .replace("education", "Education").replace("interests", "Interests")
                            .replace("gender", "Gender").replace("age", "Age")
                            for k in pe.keys()])
    pe_values = json.dumps(list(pe.values()))

    ce = d["company_enrichment"]["coverage"]
    ce_labels = json.dumps([k.replace("financialHealth", "Financial Health").replace("demographicFit", "Demographic Fit")
                            .replace("sponsorshipDeals", "Sponsorship Deals").replace("employees", "Employees")
                            .replace("salesTeam", "Sales Team").replace("marketingTeam", "Marketing Team")
                            .replace("salesProportion", "Sales Proportion").replace("marketingProportion", "Marketing Proportion")
                            .replace("execGolf", "Exec Golf Interest").replace("name", "Name")
                            for k in ce.keys()])
    ce_values = json.dumps(list(ce.values()))

    # Unique data
    ud = d["unique_data"]
    person_cats = json.dumps([c["category"] for c in ud["person"]])
    person_counts = json.dumps([c["count"] for c in ud["person"]])
    company_cats = json.dumps([c["category"] for c in ud["company"]])
    company_counts = json.dumps([c["count"] for c in ud["company"]])

    # Core table stats
    acct = d["core_tables"]["account"]
    cont = d["core_tables"]["contact"]
    actv = d["core_tables"]["activity"]

    acct_total = fmt(acct.get("total", 0))
    acct_cid_pct = acct.get("company_id_pct", 0)
    cont_total = fmt(cont.get("total", 0))
    cont_pid_pct = cont.get("person_id_pct", 0)
    actv_total = fmt(actv.get("total", 0))
    actv_7d = fmt(actv.get("last_7d", 0))
    actv_30d = fmt(actv.get("last_30d", 0))

    expansion_count = len(d["crm_plans"]["expansion"])
    unique_total = ud["person_total"] + ud["company_total"]

    # Top tables for report
    top_table_rows = ""
    for t in d["table_volumes"]["top_tables"][:10]:
        top_table_rows += f'<tr><td class="name">{t["table_schema"]}.{t["table_name"]}</td>'
        top_table_rows += f'<td class="num">{fmt(t["row_count"])}</td><td class="num">{fmt(t["size_mb"])} MB</td></tr>\n'

    # Schema inventory rows
    schema_rows = ""
    for schema, stats in d["columns"]["schema_stats"].items():
        schema_rows += f'<tr><td class="name">{schema}</td><td class="num">{stats["tables"]}</td><td class="num">{stats["columns"]}</td></tr>\n'

    # Events rows
    event_rows = ""
    for ev in d["events_collections"]["events"][:10]:
        event_rows += f'<tr><td class="name">{ev["eventType"]}</td><td class="num">{fmt(ev["cnt"])}</td></tr>\n'

    html = f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>CRED Intelligence Lab – {proj} Audit</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{
  --green:#10b981;--lime:#BEF264;--green-light:#d1fae5;--green-dark:#166534;
  --purple:#7363f3;--blue:#0091FF;--yellow:#F7B500;--orange:#F59E0B;--red:#EF4444;
  --dark:#0C1E1B;--text:rgba(9,10,11,.85);--text2:rgba(9,10,11,.5);
  --grey:#848485;--bg:#ffffff;--card:#fafafa;--border:#e5e5e5;
}}
body{{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:var(--bg);color:#090a0b;line-height:1.6;-webkit-font-smoothing:antialiased}}
.hero{{background:var(--dark);color:#fff;padding:56px 24px 60px;text-align:center;position:relative;overflow:hidden}}
.hero::before{{content:'';position:absolute;top:-200px;right:-100px;width:500px;height:500px;
  background:radial-gradient(circle,rgba(16,185,129,.12),transparent 70%);border-radius:50%}}
.hero h1{{font-size:44px;font-weight:800;letter-spacing:-.03em;line-height:1.12;margin-bottom:14px;position:relative;z-index:1}}
.hero .sub{{font-size:16px;opacity:.55;max-width:600px;margin:0 auto;position:relative;z-index:1}}
.hero .meta{{font-size:12px;opacity:.35;margin-top:14px;position:relative;z-index:1}}
.content{{max-width:860px;margin:0 auto;padding:0 24px}}
.sec{{padding:48px 0;border-bottom:1px solid var(--border)}}
.sec:last-child{{border-bottom:none}}
.sec-label{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--green);margin-bottom:8px}}
.sec-title{{font-size:28px;font-weight:800;letter-spacing:-.02em;line-height:1.2;margin-bottom:20px}}
.sec-title .hl{{color:var(--green)}}
.prose{{font-size:16px;color:var(--text);line-height:1.8;margin-bottom:16px}}
.prose:last-child{{margin-bottom:0}}
.prose strong{{color:#090a0b}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px}}
.st{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px}}
.st .v{{font-size:28px;font-weight:800;letter-spacing:-.02em;line-height:1.1}}
.st .v.g{{color:var(--green)}} .st .v.o{{color:var(--orange)}} .st .v.r{{color:var(--red)}} .st .v.b{{color:var(--blue)}}
.st .d{{font-size:12px;color:var(--grey);margin-top:4px}}
.card{{background:#fff;border:1px solid var(--border);border-radius:12px;padding:32px 32px 32px 24px;margin-bottom:20px}}
.card .ey{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--green);margin-bottom:6px}}
.card .ch{{font-size:20px;font-weight:700;letter-spacing:-.02em;margin-bottom:3px}}
.card .ch .hl{{color:var(--green)}}
.card .cd{{font-size:13px;color:var(--text2);margin-bottom:20px}}
.chart-box{{position:relative;width:100%;max-height:500px}}
.callout{{background:var(--dark);color:#fff;border-radius:12px;padding:28px 32px;margin:20px 0;display:flex;align-items:center;gap:16px}}
.callout-pip{{flex-shrink:0;min-width:48px;height:48px;background:var(--green);border-radius:10px;
  display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:800;color:#fff}}
.callout-pip.warn{{background:var(--orange)}} .callout-pip.bad{{background:var(--red)}}
.callout h3{{font-size:16px;font-weight:700;margin-bottom:3px}}
.callout p{{font-size:13px;color:rgba(255,255,255,.5);line-height:1.5}}
.validation{{background:#f0fdf4;border-left:4px solid var(--green);border-radius:8px;padding:16px 20px;margin:16px 0}}
.validation.warn{{background:#fffbeb;border-left-color:var(--orange)}}
.validation.bad{{background:#fef2f2;border-left-color:var(--red)}}
.validation h4{{font-size:13px;color:var(--green-dark);margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}}
.validation.warn h4{{color:#92400e}} .validation.bad h4{{color:#991b1b}}
.validation p,.validation ul{{font-size:14px;color:var(--text);line-height:1.6}}
.tbl{{width:100%;border-collapse:collapse;font-size:14px;margin:16px 0}}
.tbl thead th{{text-align:left;padding:10px 14px;border-bottom:2px solid var(--border);font-weight:700;font-size:12px;color:var(--grey);text-transform:uppercase;letter-spacing:.04em}}
.tbl tbody tr{{border-bottom:1px solid var(--border)}}
.tbl tbody tr:hover{{background:#f8fdf8}}
.tbl tbody td{{padding:10px 14px}}
.tbl .name{{font-weight:600}} .tbl .num{{font-variant-numeric:tabular-nums;text-align:right}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin:20px 0}}
.footer{{text-align:center;padding:32px 24px;font-size:12px;color:var(--grey);border-top:1px solid var(--border)}}
@media(max-width:640px){{
  .hero h1{{font-size:30px}} .stats{{grid-template-columns:repeat(2,1fr)}}
  .grid-2{{grid-template-columns:1fr}} .callout{{flex-direction:column;text-align:center}} .card{{padding:20px}}
}}
h3.tier-header{{margin:2rem 0 .5rem;font-size:1.1rem;letter-spacing:.04em}}
</style>
<script>
Chart.register(ChartDataLabels);
Chart.defaults.plugins.datalabels={{display:false}};
Chart.defaults.font.family="'Inter',-apple-system,sans-serif";
Chart.defaults.font.size=12;
Chart.defaults.color='#848485';
</script>
</head><body>

<div class="hero">
  <h1>{proj} <span style="color:#BEF264">Audit Report</span></h1>
  <p class="sub">Full audit of the {proj} BigQuery project: schema inventory, CRM plan analysis, enrichment coverage, competitive data differentiation, and expansion opportunities.</p>
  <div class="meta">Generated {today} &bull; CRED Intelligence Lab</div>
</div>

<div class="content">

<!-- Stat Cards -->
<div class="sec">
  <div class="stats">
    <div class="st"><div class="v g">{d['columns']['total_tables']}</div><div class="d">Tables across key schemas</div></div>
    <div class="st"><div class="v">{fmt(d['columns']['total_columns'])}</div><div class="d">Total columns catalogued</div></div>
    <div class="st"><div class="v o">{expansion_count}</div><div class="d">CRM expansion opportunities</div></div>
    <div class="st"><div class="v b">{unique_total}</div><div class="d">Unique data points vs standard CRMs</div></div>
  </div>
</div>

<!-- Schema Inventory -->
<div class="sec">
<div class="sec-label">Section 1</div>
<div class="sec-title">Schema <span class="hl">Inventory</span></div>
<p class="prose">The <code>{proj}</code> project contains <strong>{len(d['schemas']['schemas'])} schemas</strong>. Key schemas analysed:</p>
<table class="tbl">
<thead><tr><th>Schema</th><th class="num">Tables</th><th class="num">Columns</th></tr></thead>
<tbody>{schema_rows}</tbody>
</table>
</div>

<!-- Table Volumes -->
<div class="sec">
<div class="sec-label">Section 2</div>
<div class="sec-title">Table <span class="hl">Volumes</span></div>
<div class="stats" style="grid-template-columns:repeat(4,1fr)">
  <div class="st"><div class="v g">{d['table_volumes']['active_tables']}</div><div class="d">Active tables</div></div>
  <div class="st"><div class="v">{d['table_volumes']['empty_tables']}</div><div class="d">Empty tables</div></div>
  <div class="st"><div class="v">{fmt(d['table_volumes']['total_rows'])}</div><div class="d">Total rows</div></div>
  <div class="st"><div class="v">{fmt(d['table_volumes']['total_storage_mb'])} MB</div><div class="d">Total storage</div></div>
</div>
<table class="tbl">
<thead><tr><th>Table</th><th class="num">Rows</th><th class="num">Size</th></tr></thead>
<tbody>{top_table_rows}</tbody>
</table>
</div>

<!-- CRM Plan Analysis -->
<div class="sec">
<div class="sec-label">Section 3</div>
<div class="sec-title">CRM Plan <span class="hl">Gap Analysis</span></div>
<p class="prose">Complete inventory of data points that can be exposed through CRM, grouped by suggested tier.</p>
<div class="stats" style="grid-template-columns:repeat(3,1fr)">
  <div class="st"><div class="v r">{len(d['crm_plans']['crm_tagged'])}</div><div class="d">CRM-tagged data points</div></div>
  <div class="st"><div class="v g">{d['crm_plans']['insights_count']}</div><div class="d">Insights-tagged</div></div>
  <div class="st"><div class="v b">{d['crm_plans']['enrichment_count']}</div><div class="d">Enrichment-tagged</div></div>
</div>

<h3 class="tier-header" style="color:#10b981">BASE TIER &mdash; {base_total} data points</h3>
<table class="tbl">
<thead><tr><th>Data Point</th><th>Type</th><th>Insights</th><th>Enrichment</th><th>CRM Status</th></tr></thead>
<tbody>
{tier_table_rows(base_existing, "On CRM (BASE)", "#10b981", "700")}
{tier_table_rows(base_exp, "Not on CRM", "#e74c3c", "600")}
</tbody></table>

<h3 class="tier-header" style="color:#0091FF">PRO TIER &mdash; {pro_total} data points</h3>
<table class="tbl">
<thead><tr><th>Data Point</th><th>Type</th><th>Insights</th><th>Enrichment</th><th>CRM Status</th></tr></thead>
<tbody>
{tier_table_rows(pro_exp, "Not on CRM", "#e74c3c", "600")}
</tbody></table>

<h3 class="tier-header" style="color:#7363f3">CUSTOM TIER &mdash; {custom_total} data points</h3>
<table class="tbl">
<thead><tr><th>Data Point</th><th>Type</th><th>Insights</th><th>Enrichment</th><th>CRM Status</th></tr></thead>
<tbody>
{tier_table_rows(custom_existing, "On CRM (CUSTOM)", "#10b981", "700")}
{tier_table_rows(custom_exp, "Not on CRM", "#e74c3c", "600")}
</tbody></table>

<div class="validation warn" style="margin-top:2rem">
  <h4>Immediate Action</h4>
  <p>All {expansion_count} expansion candidates are already live on Insights/Enrichment. Assigning CRM tiers requires only a metadata update &mdash; no new data pipelines.</p>
</div>
</div>

<!-- Core Tables -->
<div class="sec">
<div class="sec-label">Section 4</div>
<div class="sec-title">Core CRM <span class="hl">Table Analysis</span></div>
<table class="tbl">
<thead><tr><th>Table</th><th class="num">Rows</th><th>Key Finding</th></tr></thead>
<tbody>
<tr><td class="name">Account</td><td class="num">{acct_total}</td><td>{acct_cid_pct}% have companyId linked</td></tr>
<tr><td class="name">Contact</td><td class="num">{cont_total}</td><td>{cont_pid_pct}% have personId linked</td></tr>
<tr><td class="name">Activity</td><td class="num">{actv_total}</td><td>{actv_7d} last 7 days, {actv_30d} last 30 days</td></tr>
</tbody>
</table>
</div>

<!-- Events -->
<div class="sec">
<div class="sec-label">Section 5</div>
<div class="sec-title">User Events & <span class="hl">Collections</span></div>
<p class="prose"><strong>{fmt(d['events_collections']['events_total'])}</strong> total events.
Collections: <strong>{fmt(d['events_collections']['collections'].get('total_collections', 0))}</strong> collections,
<strong>{fmt(d['events_collections']['collections'].get('total_items', 0))}</strong> items from
<strong>{fmt(d['events_collections']['collections'].get('unique_users', 0))}</strong> users.</p>
<table class="tbl">
<thead><tr><th>Event Type</th><th class="num">Count</th></tr></thead>
<tbody>{event_rows}</tbody>
</table>
</div>

<!-- Person Enrichment -->
<div class="sec">
<div class="sec-label">Section 6</div>
<div class="sec-title">Person Enrichment <span class="hl">Coverage</span></div>
<p class="prose"><strong>{fmt(d['person_enrichment']['total_records'])}</strong> person enrichment records.</p>
<div class="card">
  <div class="ey">Person Enrichment</div>
  <div class="ch">Field Coverage &mdash; <span class="hl">{fmt(d['person_enrichment']['total_records'])} Records</span></div>
  <div class="cd">Percentage of person records with each field populated</div>
  <div class="chart-box" style="height:480px"><canvas id="personChart"></canvas></div>
</div>
</div>

<!-- Company Enrichment -->
<div class="sec">
<div class="sec-label">Section 7</div>
<div class="sec-title">Company Enrichment <span class="hl">Coverage</span></div>
<p class="prose"><strong>{fmt(d['company_enrichment']['total_records'])}</strong> company enrichment records.</p>
<div class="card">
  <div class="ey">Company Enrichment</div>
  <div class="ch">Field Coverage &mdash; <span class="hl">{fmt(d['company_enrichment']['total_records'])} Records</span></div>
  <div class="cd">Percentage of company records with each field populated</div>
  <div class="chart-box" style="height:340px"><canvas id="companyChart"></canvas></div>
</div>
</div>

<!-- Unique Data Points -->
<div class="sec">
<div class="sec-label">Section 8</div>
<div class="sec-title">Competitive <span class="hl">Differentiation</span></div>
<p class="prose">CRED provides <strong>{unique_total} data points</strong> that Salesforce, HubSpot, and Dynamics 365 do not natively offer.</p>
<div class="grid-2">
<div class="card">
  <div class="ey">Person-Level</div>
  <div class="ch"><span class="hl">{ud['person_total']}</span> Unique Data Points</div>
  <div class="cd">Not available in standard CRMs</div>
  <div class="chart-box" style="height:220px"><canvas id="personUniqueChart"></canvas></div>
</div>
<div class="card">
  <div class="ey">Company-Level</div>
  <div class="ch"><span class="hl">{ud['company_total']}</span> Unique Data Points</div>
  <div class="cd">Not available in standard CRMs</div>
  <div class="chart-box" style="height:250px"><canvas id="companyUniqueChart"></canvas></div>
</div>
</div>
</div>

<!-- Data Sources -->
<div class="sec">
<div class="sec-label">Section 9</div>
<div class="sec-title">Platform <span class="hl">Data Sources</span></div>
<div class="stats" style="grid-template-columns:repeat(3,1fr)">
  <div class="st"><div class="v g">{d['data_sources']['count']}</div><div class="d">Registered data sources</div></div>
  <div class="st"><div class="v">{fmt(d['person_enrichment']['total_records'])}</div><div class="d">Person enrichment records</div></div>
  <div class="st"><div class="v">{fmt(d['company_enrichment']['total_records'])}</div><div class="d">Company enrichment records</div></div>
</div>
<p class="prose">Unipile/LinkedIn status: <strong>{d['unipile']['status']}</strong> ({d['unipile']['total']} connections)</p>
</div>

</div><!-- /content -->

<div class="footer">CRED Intelligence Lab &bull; {proj} Audit &bull; {today}</div>

<script>
var coverageOpts = function(maxVal) {{
  return {{
    indexAxis:'y', responsive:true, maintainAspectRatio:false,
    layout:{{padding:{{right:60}}}},
    plugins:{{legend:{{display:false}},
      datalabels:{{display:true, anchor:'end', align:'right', clip:false,
        font:{{weight:'700',size:12}}, formatter:function(v){{return v+'%'}},
        color:function(ctx){{var v=ctx.dataset.data[ctx.dataIndex]; return v>=50?'#166534':v>=25?'#92400e':'#991b1b'}}}}}},
    scales:{{
      x:{{min:0, max:maxVal, grid:{{display:false}}, ticks:{{stepSize:25, callback:function(v){{return v+'%'}}}}, border:{{display:false}}}},
      y:{{grid:{{display:false}}, ticks:{{font:{{size:13,weight:'600'}}}}, border:{{display:false}}}}}}
  }};
}};

new Chart(document.getElementById('personChart'),{{
  type:'bar',
  data:{{labels:{pe_labels}, datasets:[{{data:{pe_values},
    backgroundColor:function(ctx){{var v=ctx.raw;return v>=75?'#10b981':v>=50?'#4CAF50':v>=25?'#F59E0B':'#EF4444'}},
    borderRadius:4, barPercentage:.75, categoryPercentage:.85}}]}},
  options:coverageOpts(100)
}});

new Chart(document.getElementById('companyChart'),{{
  type:'bar',
  data:{{labels:{ce_labels}, datasets:[{{data:{ce_values},
    backgroundColor:function(ctx){{var v=ctx.raw;return v>=75?'#10b981':v>=50?'#4CAF50':v>=25?'#F59E0B':'#EF4444'}},
    borderRadius:4, barPercentage:.75, categoryPercentage:.85}}]}},
  options:coverageOpts(100)
}});

new Chart(document.getElementById('personUniqueChart'),{{
  type:'bar',
  data:{{labels:{person_cats}, datasets:[{{data:{person_counts},
    backgroundColor:['#0091FF','#7363f3','#F59E0B','#10b981','#EC4899'],
    borderRadius:6, barPercentage:.7, categoryPercentage:.85}}]}},
  options:{{indexAxis:'y', responsive:true, maintainAspectRatio:false,
    layout:{{padding:{{right:50}}}},
    plugins:{{legend:{{display:false}},
      datalabels:{{display:true, anchor:'end', align:'right', clip:false,
        font:{{weight:'800',size:15}}, color:'#090a0b',
        formatter:function(v){{return v+' fields'}}}}}},
    scales:{{x:{{min:0, max:{max(c['count'] for c in ud['person'])+1}, grid:{{color:'rgba(0,0,0,.05)'}}, ticks:{{stepSize:1}}, border:{{display:false}}}},
      y:{{grid:{{display:false}}, ticks:{{font:{{size:13,weight:'600'}}}}, border:{{display:false}}}}}}}}
}});

new Chart(document.getElementById('companyUniqueChart'),{{
  type:'bar',
  data:{{labels:{company_cats}, datasets:[{{data:{company_counts},
    backgroundColor:['#10b981','#0091FF','#7363f3','#F59E0B','#F7B500','#EC4899'],
    borderRadius:6, barPercentage:.7, categoryPercentage:.85}}]}},
  options:{{indexAxis:'y', responsive:true, maintainAspectRatio:false,
    layout:{{padding:{{right:50}}}},
    plugins:{{legend:{{display:false}},
      datalabels:{{display:true, anchor:'end', align:'right', clip:false,
        font:{{weight:'800',size:15}}, color:'#090a0b',
        formatter:function(v){{return v+' fields'}}}}}},
    scales:{{x:{{min:0, max:{max(c['count'] for c in ud['company'])+1}, grid:{{color:'rgba(0,0,0,.05)'}}, ticks:{{stepSize:1}}, border:{{display:false}}}},
      y:{{grid:{{display:false}}, ticks:{{font:{{size:13,weight:'600'}}}}, border:{{display:false}}}}}}}}
}});
</script>

</body></html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html)
    print(f"\n✅ Report saved to: {output_path}")


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    proj = args.project
    billing = args.billing_project
    today = datetime.now().strftime("%Y-%m-%d")

    if args.output:
        output_path = args.output
    else:
        repo_root = Path(__file__).resolve().parent.parent
        reports_dir = repo_root / "analysis_notebooks" / "reports" / "output"
        if not reports_dir.parent.exists():
            reports_dir = repo_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        output_path = str(reports_dir / f"{proj}_audit_{today}.html")

    print(f"\n{'='*60}")
    print(f"  Commercial DB Audit: {proj}")
    print(f"  Billing project: {billing}")
    print(f"  Report output: {output_path}")
    print(f"{'='*60}\n")

    bq = connect(billing)

    data = {
        "project": proj,
        "billing_project": billing,
        "date": today,
        "schemas": section_schemas(bq, proj),
        "columns": section_columns(bq, proj, args.schemas),
        "table_volumes": section_table_volumes(bq, proj),
        "crm_plans": section_crm_plans(bq, billing),
        "core_tables": section_core_tables(bq, proj),
        "events_collections": section_events_collections(bq, proj),
        "unipile": section_unipile(bq, proj),
        "person_enrichment": section_person_enrichment(bq, proj),
        "company_enrichment": section_company_enrichment(bq, proj),
        "data_sources": section_data_sources(bq, billing),
        "unique_data": section_unique_data(),
    }

    generate_report(data, output_path)
    print(f"\nOpen: file://{output_path}")


if __name__ == "__main__":
    main()
