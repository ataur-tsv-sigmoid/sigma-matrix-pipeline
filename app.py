"""
Sigma Matrix — AI-Powered Data Pipeline Dashboard
Company  : Sigma TechZone (Logistics)
Author   : Senior Data Engineer
Stack    : Streamlit · AWS S3 · Glue · Athena · Bedrock (Nova Lite)
"""

import time
import json
import io
import re
from datetime import datetime, timezone   # needed for alert timestamps

import boto3
import pandas as pd
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sigma Matrix Dashboard",
    layout="wide",
    page_icon="⚡",
    initial_sidebar_state="collapsed",
)

# ── Global CSS — accent colour #2DC653 ─────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        font-size: 16px;
    }

    /* Dark background */
    .stApp { background: #0d1117; color: #e6edf3; }

    /* ── Gradient hero header ── */
    .hero-header {
        background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #1a2332 100%);
        border: 1px solid #2DC653;
        border-radius: 16px;
        padding: 28px 36px;
        margin-bottom: 28px;
        position: relative;
        overflow: hidden;
    }
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -10%;
        width: 300px;
        height: 300px;
        background: radial-gradient(circle, rgba(45,198,83,0.12) 0%, transparent 70%);
        pointer-events: none;
    }
    .hero-title {
        font-size: 2.2rem;
        font-weight: 800;
        color: #2DC653;
        margin: 0 0 4px 0;
        letter-spacing: -0.5px;
    }
    .hero-subtitle {
        font-size: 1rem;
        color: #8b949e;
        margin: 0;
    }
    .hero-badge {
        display: inline-block;
        background: rgba(45,198,83,0.15);
        color: #2DC653;
        border: 1px solid #2DC653;
        border-radius: 20px;
        padding: 2px 12px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-top: 10px;
    }

    /* ── Tab styling ── */
    .stTabs [data-baseweb="tab-list"] {
        background: #161b22;
        border-radius: 12px;
        padding: 4px;
        border: 1px solid #21262d;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #8b949e;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.95rem;
        padding: 8px 20px;
        transition: all 0.2s ease;
    }
    .stTabs [aria-selected="true"] {
        background: #2DC653 !important;
        color: #0d1117 !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #2DC653;
        background: rgba(45,198,83,0.1) !important;
    }
    .stTabs [data-baseweb="tab-highlight"] { display: none; }

    /* ── Buttons ── */
    .stButton > button {
        background: linear-gradient(135deg, #2DC653, #22a045) !important;
        color: #0d1117 !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 700 !important;
        font-size: 0.95rem !important;
        padding: 10px 24px !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 4px 15px rgba(45,198,83,0.3) !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(45,198,83,0.4) !important;
    }
    .stButton > button:active { transform: translateY(0) !important; }

    /* Secondary / outline buttons for quick questions */
    .qq-btn > button {
        background: rgba(45,198,83,0.08) !important;
        color: #2DC653 !important;
        border: 1px solid #2DC653 !important;
        font-weight: 500 !important;
        font-size: 0.82rem !important;
        padding: 6px 12px !important;
        box-shadow: none !important;
    }
    .qq-btn > button:hover {
        background: rgba(45,198,83,0.18) !important;
        transform: none !important;
        box-shadow: none !important;
    }

    /* ── Metric cards ── */
    [data-testid="metric-container"] {
        background: #161b22;
        border: 1px solid #21262d;
        border-radius: 12px;
        padding: 16px 20px;
        transition: border-color 0.2s;
    }
    [data-testid="metric-container"]:hover { border-color: #2DC653; }
    [data-testid="stMetricLabel"] { color: #2DC653 !important; font-weight: 600; font-size: 0.85rem; }
    [data-testid="stMetricValue"] { color: #e6edf3 !important; font-size: 1.6rem; font-weight: 700; }

    /* ── Step cards ── */
    .step-card {
        background: #161b22;
        border: 1px solid #21262d;
        border-radius: 10px;
        padding: 14px 20px;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 12px;
        font-size: 0.95rem;
    }
    .step-ok  { border-left: 4px solid #2DC653; }
    .step-err { border-left: 4px solid #f85149; }
    .step-pending { border-left: 4px solid #30363d; color: #8b949e; }

    /* ── Section headers ── */
    .section-header {
        color: #2DC653;
        font-size: 1.3rem;
        font-weight: 700;
        margin: 24px 0 12px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .section-divider {
        border: none;
        border-top: 1px solid #21262d;
        margin: 20px 0;
    }

    /* ── Code blocks ── */
    .stCode { border-radius: 10px !important; }

    /* ── Inputs ── */
    .stTextInput > div > div > input, .stSelectbox > div > div {
        background: #161b22 !important;
        border: 1px solid #30363d !important;
        border-radius: 8px !important;
        color: #e6edf3 !important;
    }
    .stTextInput > div > div > input:focus {
        border-color: #2DC653 !important;
        box-shadow: 0 0 0 2px rgba(45,198,83,0.2) !important;
    }

    /* ── AI insight box ── */
    .ai-insight {
        background: linear-gradient(135deg, rgba(45,198,83,0.08), rgba(45,198,83,0.03));
        border: 1px solid rgba(45,198,83,0.3);
        border-radius: 12px;
        padding: 18px 22px;
        margin-top: 16px;
    }
    .ai-label {
        color: #2DC653;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
        margin-bottom: 8px;
    }
    .ai-text { color: #e6edf3; font-size: 0.95rem; line-height: 1.6; }

    /* ── Progress bar ── */
    .stProgress > div > div > div { background: #2DC653 !important; }

    /* ── Dataframe ── */
    .stDataFrame { border-radius: 10px; overflow: hidden; }

    /* ── Alert / info boxes ── */
    .stSuccess { background: rgba(45,198,83,0.1) !important; border-color: #2DC653 !important; }
    .stWarning { background: rgba(255,196,0,0.1) !important; }
    .stError   { background: rgba(248,81,73,0.1) !important; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0d1117; }
    ::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #2DC653; }

    /* ══ Feature 3 — Data Quality CSS ══════════════════════════════════ */

    /* ── DQ Score ring container ── */
    .dq-score-wrapper {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 24px 0 16px;
    }
    .dq-score-ring {
        position: relative;
        width: 160px;
        height: 160px;
    }
    .dq-score-ring svg {
        transform: rotate(-90deg);
    }
    .dq-score-ring .ring-bg {
        fill: none;
        stroke: #21262d;
        stroke-width: 14;
    }
    .dq-score-ring .ring-fg {
        fill: none;
        stroke-width: 14;
        stroke-linecap: round;
        transition: stroke-dashoffset 1s ease, stroke 0.4s ease;
    }
    .dq-score-value {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        font-size: 2rem;
        font-weight: 800;
        color: #e6edf3;
        text-align: center;
        line-height: 1;
    }
    .dq-score-label {
        font-size: 0.72rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
        text-align: center;
    }
    .dq-score-title {
        font-size: 1rem;
        font-weight: 700;
        margin-top: 10px;
        text-align: center;
    }

    /* ── DQ Dimension cards ── */
    .dq-dim-card {
        background: #161b22;
        border: 1px solid #21262d;
        border-radius: 12px;
        padding: 18px 20px;
        text-align: center;
        transition: border-color 0.2s, transform 0.2s;
        height: 100%;
    }
    .dq-dim-card:hover {
        border-color: #2DC653;
        transform: translateY(-2px);
    }
    .dq-dim-icon {
        font-size: 1.8rem;
        margin-bottom: 6px;
    }
    .dq-dim-name {
        font-size: 0.78rem;
        color: #8b949e;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 4px;
    }
    .dq-dim-value {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1;
        margin-bottom: 4px;
    }
    .dq-dim-desc {
        font-size: 0.78rem;
        color: #8b949e;
    }

    /* ── DQ status badge ── */
    .dq-badge {
        display: inline-block;
        border-radius: 20px;
        padding: 3px 10px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    .dq-badge-healthy  { background: rgba(45,198,83,0.15);  color: #2DC653; border: 1px solid #2DC653; }
    .dq-badge-warning  { background: rgba(255,196,0,0.15);  color: #ffc400; border: 1px solid #ffc400; }
    .dq-badge-critical { background: rgba(248,81,73,0.15);  color: #f85149; border: 1px solid #f85149; }

    /* ── DQ issues table styling ── */
    .dq-issues-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.88rem;
    }
    .dq-issues-table th {
        background: #161b22;
        color: #2DC653;
        font-weight: 700;
        padding: 10px 14px;
        text-align: left;
        border-bottom: 1px solid #21262d;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.6px;
    }
    .dq-issues-table td {
        padding: 10px 14px;
        border-bottom: 1px solid #21262d;
        color: #e6edf3;
        vertical-align: middle;
    }
    .dq-issues-table tr:hover td { background: rgba(45,198,83,0.04); }
    .dq-issues-table tr:last-child td { border-bottom: none; }

    /* ── DQ trend indicator ── */
    .dq-trend-good { color: #2DC653; font-weight: 700; }
    .dq-trend-bad  { color: #f85149; font-weight: 700; }
    .dq-trend-neutral { color: #8b949e; font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Constants ──────────────────────────────────────────────────────────────────
BUCKET        = "sigma-matrix-bucket"
GLUE_JOB      = "sigma-matrix-etl"
ATHENA_DB     = "sigmamatrixdb"
TBL_ORDERS    = "sigmamatrixorders"
TBL_CUSTOMERS = "sigmamatrixcustomers"
TBL_PRODUCTS  = "sigmamatrixproducts"
GLUE_ROLE     = "SigmaGlueServiceRole"
REGION        = "us-east-1"
ATHENA_OUT    = f"s3://{BUCKET}/athena-results/"
BEDROCK_MODEL = "us.amazon.nova-lite-v1:0"

# ── Feature 1 — Alert configuration ───────────────────────────────────────────
# SNS topic ARN is read from Streamlit secrets (secrets.toml) with a safe fallback.
# Set DQ_ISSUE_THRESHOLD to the max tolerable total data-quality anomalies per run.
SNS_TOPIC_ARN     = st.secrets.get("SNS_TOPIC_ARN", "")   # e.g. arn:aws:sns:us-east-1:123456789:sigma-matrix-alerts
DQ_ISSUE_THRESHOLD = 10   # configurable: trigger alert when total DQ issues exceed this value

DAYS = [
    ("Day 1 — 2026-05-01", "2026-05-01", "orders_day1.csv"),
    ("Day 2 — 2026-05-02", "2026-05-02", "orders_day2.csv"),
    ("Day 3 — 2026-05-03", "2026-05-03", "orders_day3.csv"),
    ("Day 4 — 2026-05-04", "2026-05-04", "orders_day4.csv"),
    ("Day 5 — 2026-05-05", "2026-05-05", "orders_day5.csv"),
]

QUICK_QUESTIONS = [
    "Top 5 cities by revenue",
    "Daily order trend",
    "High value orders per day",
    "Top 3 payment methods by order count",
    "Average order amount by city",
]

ORDERS_SCHEMA = """sigmamatrixorders (
    order_id STRING, customer_id STRING, product_id STRING,
    quantity INT, amount DOUBLE, status STRING,
    payment_method STRING, city STRING, created_at STRING,
    processed_at STRING, is_high_value STRING,
    date STRING  -- partition column
)"""

CUSTOMERS_SCHEMA = """sigmamatrixcustomers (
    customer_id STRING, name STRING, email STRING,
    phone STRING, city STRING, tier STRING, signup_date STRING
)"""

PRODUCTS_SCHEMA = """sigmamatrixproducts (
    product_id STRING, name STRING, category STRING,
    price DOUBLE, stock_quantity INT, is_active STRING
)"""

# ── AWS clients ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_clients():
    s3      = boto3.client("s3",      region_name=REGION)
    glue    = boto3.client("glue",    region_name=REGION)
    athena  = boto3.client("athena",  region_name=REGION)
    bedrock = boto3.client("bedrock-runtime", region_name=REGION)
    return s3, glue, athena, bedrock

s3, glue, athena, bedrock = get_clients()

# ── Helper: Athena run query ───────────────────────────────────────────────────
def run_athena_query(sql: str, database: str = ATHENA_DB) -> pd.DataFrame:
    """Execute SQL on Athena, poll until done, return DataFrame."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUT},
    )
    qid = resp["QueryExecutionId"]

    for _ in range(120):
        state = athena.get_query_execution(QueryExecutionId=qid)
        status = state["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            break
        if status == "FAILED":
            reason = state["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena query FAILED: {reason}")
        time.sleep(2)
    else:
        raise TimeoutError("Athena query timed out after 4 minutes.")

    results = athena.get_query_results(QueryExecutionId=qid)
    cols = [c["Label"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    all_rows = results["ResultSet"]["Rows"]

    # Detect and skip header row only for SELECT (compares first row to col names)
    if all_rows and [f.get("VarCharValue", "") for f in all_rows[0]["Data"]] == cols:
        all_rows = all_rows[1:]

    data = [[field.get("VarCharValue", "") for field in row["Data"]] for row in all_rows]
    return pd.DataFrame(data, columns=cols)


# ── Helper: Bedrock converse ───────────────────────────────────────────────────
def bedrock_converse(prompt: str) -> str:
    response = bedrock.converse(
        modelId=BEDROCK_MODEL,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 500, "temperature": 0.0},
    )
    return response["output"]["message"]["content"][0]["text"].strip()


# ── Helper: format currency ────────────────────────────────────────────────────
MONEY_COLS = {"amount", "revenue", "total_sales", "total_revenue", "avg_amount",
              "average_amount", "total_amount", "sales"}

def format_df_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Format money columns as ₹X,XX,XXX with no decimals."""
    df2 = df.copy()
    for col in df2.columns:
        if col.lower().replace(" ", "_") in MONEY_COLS or "amount" in col.lower() or "revenue" in col.lower():
            def _fmt(x):
                try:
                    return f"₹{int(round(float(x))):,}"
                except (ValueError, TypeError):
                    return x
            df2[col] = df2[col].apply(_fmt)
    return df2


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 3 — DATA QUALITY HELPER
# compute_dq_score() is a pure Python function — no AWS calls, no side-effects.
# It derives the three quality dimensions directly from the report dict that
# the Glue ETL already writes to S3 as quality_report_<date>.json.
# ══════════════════════════════════════════════════════════════════════════════

def compute_dq_score(report: dict) -> dict:
    """
    Derive data-quality dimension scores from a single Glue quality report.

    Dimensions
    ----------
    completeness  : % rows that survived (had a valid customer_id)
    validity      : % rows without negative amounts
    uniqueness    : % rows without duplicate order_ids
    overall_score : weighted mean (equal weight across the three dimensions)

    Returns a dict with all raw counts plus computed percentages.
    """
    input_rows   = max(report.get("input_rows",  1), 1)   # guard /0
    output_rows  = report.get("output_rows",          0)
    null_ids     = report.get("null_customer_ids",    0)
    neg_amounts  = report.get("negative_amounts",     0)
    dup_ids      = report.get("duplicate_order_ids",  0)
    rows_dropped = report.get("rows_dropped",         0)

    completeness_pct = round(max(output_rows / input_rows * 100, 0), 1)
    validity_pct     = round(max((1 - neg_amounts  / input_rows) * 100, 0), 1)
    uniqueness_pct   = round(max((1 - dup_ids      / input_rows) * 100, 0), 1)
    null_rate_pct    = round(null_ids / input_rows * 100, 2)

    overall_score = round(
        (completeness_pct + validity_pct + uniqueness_pct) / 3, 1
    )

    if overall_score >= 90:
        grade = "HEALTHY"
    elif overall_score >= 70:
        grade = "WARNING"
    else:
        grade = "CRITICAL"

    return {
        "date"             : report.get("date", "—"),
        "input_rows"       : input_rows,
        "output_rows"      : output_rows,
        "rows_dropped"     : rows_dropped,
        "null_ids"         : null_ids,
        "neg_amounts"      : neg_amounts,
        "dup_ids"          : dup_ids,
        "null_rate_pct"    : null_rate_pct,
        "completeness_pct" : completeness_pct,
        "validity_pct"     : validity_pct,
        "uniqueness_pct"   : uniqueness_pct,
        "overall_score"    : overall_score,
        "grade"            : grade,
    }


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 1 — EMAIL ALERTING VIA AWS SNS
# Architecture is modular: send_alert() is the single dispatch point.
# To add Slack later, insert a Slack webhook call inside send_alert() alongside
# the existing SNS publish — no other code changes required.
# ══════════════════════════════════════════════════════════════════════════════

def send_alert(
    *,
    channel: str = "sns",          # extensible: "sns" | "slack" (future)
    severity: str,                 # "CRITICAL" | "WARNING" | "INFO"
    subject: str,
    job_name: str,
    run_id: str,
    date_partition: str,
    reason: str,
    timestamp: str,
) -> bool:
    """
    Dispatch an alert to the configured channel(s).

    Currently supports AWS SNS only.  Slack integration can be added
    by appending a requests.post() call inside this function without
    touching any caller code.

    Returns True on successful dispatch, False on failure.
    """
    # ── Build the alert message body ──────────────────────────────────────────
    message_body = (
        f"🚨 SIGMA MATRIX PIPELINE ALERT\n"
        f"{'=' * 45}\n"
        f"Severity      : {severity}\n"
        f"Job Name      : {job_name}\n"
        f"Run ID        : {run_id}\n"
        f"Date Partition: {date_partition}\n"
        f"Timestamp     : {timestamp}\n"
        f"Reason        : {reason}\n"
        f"{'=' * 45}\n"
        f"Please investigate in the AWS Glue console or the pipeline dashboard."
    )

    # ── SNS channel ───────────────────────────────────────────────────────────
    if channel == "sns":
        if not SNS_TOPIC_ARN:
            # SNS not configured — log gracefully and skip
            st.info("ℹ️ SNS_TOPIC_ARN not configured in secrets.toml — alert skipped.")
            return False
        try:
            sns_client = boto3.client("sns", region_name=REGION)
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],   # SNS subject max 100 chars
                Message=message_body,
                MessageAttributes={
                    "severity": {
                        "DataType": "String",
                        "StringValue": severity,
                    }
                },
            )
            return True
        except Exception as sns_err:
            # Log but don't crash the main pipeline flow
            st.warning(f"⚠️ Alert dispatch failed (SNS error): {sns_err}")
            return False

    # ── Placeholder: Slack channel (future) ───────────────────────────────────
    # if channel == "slack":
    #     webhook_url = st.secrets.get("SLACK_WEBHOOK_URL", "")
    #     if webhook_url:
    #         import requests
    #         requests.post(webhook_url, json={"text": message_body})
    #     return True

    return False


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 2 — QUERY HISTORY HELPERS
# All history is stored in st.session_state["query_history"] as a list of dicts.
# Maximum 10 entries are retained (oldest evicted first — FIFO).
# ══════════════════════════════════════════════════════════════════════════════

QUERY_HISTORY_MAX = 10   # maximum retained history entries

def add_to_query_history(question: str, sql: str) -> None:
    """
    Prepend a new query record to session_state["query_history"].
    Evicts the oldest entry when the list exceeds QUERY_HISTORY_MAX.
    """
    if "query_history" not in st.session_state:
        st.session_state["query_history"] = []

    entry = {
        "question"  : question,
        "sql"       : sql,
        "timestamp" : datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    }

    # Prepend so the newest entry is always at index 0
    st.session_state["query_history"].insert(0, entry)

    # Enforce the rolling window cap
    st.session_state["query_history"] = st.session_state["query_history"][:QUERY_HISTORY_MAX]


# ── Hero banner ────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="hero-header">
      <div class="hero-title">⚡ Sigma Matrix</div>
      <div class="hero-subtitle">AI-Powered Data Pipeline Dashboard · AWS Glue · Athena · Bedrock</div>
      <span class="hero-badge">🟢 LIVE</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🔧 Setup Pipeline", "📦 Daily Load", "🔍 Ask Your Data",
     "📊 Pipeline Health", "🛡️ Data Quality"]
)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — Setup Pipeline
# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">🔧 Sigma Matrix — Pipeline Setup</div>', unsafe_allow_html=True)
    st.markdown(
        "Deploy the full data pipeline infrastructure: S3 bucket, Glue ETL job, and Athena tables.",
        help="Run once to initialise all AWS resources."
    )

    if st.button("🚀 Deploy Pipeline", key="btn_deploy"):
        steps = [
            "Create S3 bucket",
            "Upload Glue ETL script",
            "Upload reference data (raw + processed)",
            "Create / recreate Glue job",
            "Create Athena database",
            "Create orders table",
            "Create customers table",
            "Create products table",
        ]
        results = {}

        progress_bar = st.progress(0)
        status_placeholder = st.empty()

        def update_ui(step_idx, ok, message):
            results[steps[step_idx]] = (ok, message)
            progress_bar.progress((step_idx + 1) / len(steps))
            html = ""
            for i, s in enumerate(steps):
                if s in results:
                    ok_r, msg = results[s]
                    icon  = "✅" if ok_r else "❌"
                    cls   = "step-ok" if ok_r else "step-err"
                    color = "#2DC653" if ok_r else "#f85149"
                    html += (
                        f'<div class="step-card {cls}">'
                        f'<span style="font-size:1.2rem">{icon}</span>'
                        f'<span><strong>Step {i+1}:</strong> {s}</span>'
                        f'<span style="color:{color};margin-left:auto;font-size:0.85rem">{msg}</span>'
                        f'</div>'
                    )
                else:
                    html += (
                        f'<div class="step-card step-pending">'
                        f'<span style="font-size:1.2rem">⏳</span>'
                        f'<span><strong>Step {i+1}:</strong> {s}</span>'
                        f'</div>'
                    )
            status_placeholder.markdown(html, unsafe_allow_html=True)

        # ── Step 1: Create S3 bucket ──────────────────────────────────────────
        try:
            existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            if BUCKET not in existing:
                # us-east-1 must NOT pass CreateBucketConfiguration
                s3.create_bucket(Bucket=BUCKET)
                update_ui(0, True, "Bucket created")
            else:
                update_ui(0, True, "Already exists")
        except Exception as e:
            update_ui(0, False, str(e)[:80])

        # ── Step 2: Upload Glue script ────────────────────────────────────────
        try:
            with open("glue_scripts/etl.py", "rb") as f:
                s3.put_object(Bucket=BUCKET, Key="glue-scripts/etl.py", Body=f.read())
            update_ui(1, True, "glue-scripts/etl.py uploaded")
        except Exception as e:
            update_ui(1, False, str(e)[:80])

        # ── Step 3: Upload reference data ─────────────────────────────────────
        try:
            for fname, raw_key, processed_key in [
                ("data/customers.csv", "raw/customers.csv",  "processed/customers/customers.csv"),
                ("data/products.csv",  "raw/products.csv",   "processed/products/products.csv"),
            ]:
                with open(fname, "rb") as f:
                    data_bytes = f.read()
                s3.put_object(Bucket=BUCKET, Key=raw_key,       Body=data_bytes)
                s3.put_object(Bucket=BUCKET, Key=processed_key, Body=data_bytes)
            update_ui(2, True, "customers.csv + products.csv uploaded to raw/ & processed/")
        except Exception as e:
            update_ui(2, False, str(e)[:80])

        # ── Step 4: Create Glue job (always delete-then-create) ───────────────
        try:
            existing_jobs = glue.list_jobs().get("JobNames", [])
            if GLUE_JOB in existing_jobs:
                glue.delete_job(JobName=GLUE_JOB)
            glue.create_job(
                Name=GLUE_JOB,
                Role=GLUE_ROLE,
                Command={
                    "Name":           "pythonshell",
                    "ScriptLocation": f"s3://{BUCKET}/glue-scripts/etl.py",
                    "PythonVersion":  "3",
                },
                GlueVersion="1.0",
                MaxCapacity=0.0625,
                MaxRetries=0,
                Timeout=10,
                ExecutionProperty={"MaxConcurrentRuns": 5},
                DefaultArguments={"--additional-python-modules": "pandas"},
            )
            update_ui(3, True, "Glue job created (GlueVersion=1.0, MaxCapacity=0.0625)")
        except Exception as e:
            update_ui(3, False, str(e)[:80])

        # ── Step 5: Create Athena DB ──────────────────────────────────────────
        try:
            run_athena_query(
                f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB}",
                database="default",
            )
            update_ui(4, True, f"Database '{ATHENA_DB}' ready")
        except Exception as e:
            update_ui(4, False, str(e)[:80])

        # ── Step 6: Create orders table (IF NOT EXISTS) ───────────────────────
        try:
            run_athena_query(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{TBL_ORDERS} (
                    order_id        STRING,
                    customer_id     STRING,
                    product_id      STRING,
                    quantity        INT,
                    amount          DOUBLE,
                    status          STRING,
                    payment_method  STRING,
                    city            STRING,
                    created_at      STRING,
                    processed_at    STRING,
                    is_high_value   STRING
                )
                PARTITIONED BY (date STRING)
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                LOCATION 's3://{BUCKET}/processed/orders/'
                TBLPROPERTIES ('skip.header.line.count'='1')
            """)
            update_ui(5, True, f"Table '{TBL_ORDERS}' ready")
        except Exception as e:
            update_ui(5, False, str(e)[:80])

        # ── Step 7: Drop + Create customers table ────────────────────────────
        try:
            run_athena_query(f"DROP TABLE IF EXISTS {ATHENA_DB}.{TBL_CUSTOMERS}")
            run_athena_query(f"""
                CREATE EXTERNAL TABLE {ATHENA_DB}.{TBL_CUSTOMERS} (
                    customer_id STRING,
                    name        STRING,
                    email       STRING,
                    phone       STRING,
                    city        STRING,
                    tier        STRING,
                    signup_date STRING
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                LOCATION 's3://{BUCKET}/processed/customers/'
                TBLPROPERTIES ('skip.header.line.count'='1')
            """)
            update_ui(6, True, f"Table '{TBL_CUSTOMERS}' ready")
        except Exception as e:
            update_ui(6, False, str(e)[:80])

        # ── Step 8: Drop + Create products table ─────────────────────────────
        try:
            run_athena_query(f"DROP TABLE IF EXISTS {ATHENA_DB}.{TBL_PRODUCTS}")
            run_athena_query(f"""
                CREATE EXTERNAL TABLE {ATHENA_DB}.{TBL_PRODUCTS} (
                    product_id     STRING,
                    name           STRING,
                    category       STRING,
                    price          DOUBLE,
                    stock_quantity INT,
                    is_active      STRING
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                LOCATION 's3://{BUCKET}/processed/products/'
                TBLPROPERTIES ('skip.header.line.count'='1')
            """)
            update_ui(7, True, f"Table '{TBL_PRODUCTS}' ready")
        except Exception as e:
            update_ui(7, False, str(e)[:80])

        all_ok = all(v[0] for v in results.values())
        if all_ok:
            st.success("🎉 Pipeline deployed successfully! All 8 steps completed.")
        else:
            st.error("⚠️ Pipeline deployment completed with some errors. Check step details above.")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — Daily Load
# ─────────────────────────────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-header">📦 Daily ETL Load</div>', unsafe_allow_html=True)

    day_labels  = [d[0] for d in DAYS]
    day_sel_idx = st.selectbox("Select Day to Load", range(len(DAYS)),
                               format_func=lambda i: DAYS[i][0],
                               key="sel_day")

    sel_label, sel_date, sel_file = DAYS[day_sel_idx]

    st.markdown(
        f'<div style="color:#8b949e;font-size:0.9rem;margin-bottom:16px">'
        f'Selected: <strong style="color:#2DC653">{sel_label}</strong> · '
        f'File: <code>data/{sel_file}</code></div>',
        unsafe_allow_html=True,
    )

    if st.button(f"▶️ Run ETL for {sel_label}", key="btn_run_etl"):
        col_prog, col_status = st.columns([3, 1])
        prog_bar = col_prog.progress(0)
        status_txt = col_status.empty()

        # Step 1: Upload raw orders file
        with st.spinner(f"⬆️ Uploading data/{sel_file} to S3…"):
            try:
                with open(f"data/{sel_file}", "rb") as f:
                    raw_bytes = f.read()
                raw_key = f"raw/orders/date={sel_date}/orders.csv"
                s3.put_object(Bucket=BUCKET, Key=raw_key, Body=raw_bytes)
                st.success(f"✅ Uploaded → s3://{BUCKET}/{raw_key}")
            except Exception as e:
                st.error(f"❌ Upload failed: {e}")
                st.stop()

        # Step 2: Trigger Glue job
        with st.spinner("🚀 Triggering Glue job…"):
            try:
                run_resp = glue.start_job_run(
                    JobName=GLUE_JOB,
                    Arguments={
                        "--job_type":       "orders",
                        "--bucket_name":    BUCKET,
                        "--date_partition": sel_date,
                    },
                )
                run_id = run_resp["JobRunId"]
                st.info(f"Glue Job Run ID: `{run_id}`")
            except Exception as e:
                st.error(f"❌ Failed to start Glue job: {e}")
                st.stop()

        # Step 3: Poll with progress bar
        st.markdown('<div class="section-header" style="font-size:1rem">⏳ Monitoring Glue Job…</div>',
                    unsafe_allow_html=True)
        final_state = None
        for poll in range(40):
            try:
                job_run = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)
                state   = job_run["JobRun"]["JobRunState"]
            except Exception as e:
                st.error(f"Poll error: {e}")
                break

            prog_val = min((poll + 1) / 40, 0.99)
            prog_bar.progress(prog_val)
            status_txt.markdown(
                f'<div style="color:#2DC653;font-weight:700;padding-top:8px">{state}</div>',
                unsafe_allow_html=True,
            )

            if state in ("SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"):
                final_state = state
                break
            time.sleep(3)

        prog_bar.progress(1.0)

        # Step 4 / 5: Handle result
        if final_state == "SUCCEEDED":
            st.success("✅ Glue job SUCCEEDED!")

            # MSCK REPAIR
            with st.spinner("🔧 Running MSCK REPAIR TABLE…"):
                try:
                    run_athena_query(f"MSCK REPAIR TABLE {ATHENA_DB}.{TBL_ORDERS}")
                    st.success("✅ Partition metadata refreshed.")
                except Exception as e:
                    st.warning(f"MSCK REPAIR warning: {e}")

            # Step 6: Read quality report
            report_key = f"reports/quality_report_{sel_date}.json"
            try:
                obj     = s3.get_object(Bucket=BUCKET, Key=report_key)
                report  = json.loads(obj["Body"].read().decode("utf-8"))

                st.markdown('<div class="section-header" style="font-size:1.1rem">📋 Quality Report</div>',
                            unsafe_allow_html=True)

                # Step 7: Metric cards
                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("📥 Input Rows",         report.get("input_rows", "—"))
                m2.metric("📤 Output Rows",         report.get("output_rows", "—"))
                m3.metric("🗑️ Rows Dropped",        report.get("rows_dropped", "—"))
                m4.metric("🚫 Null Customer IDs",   report.get("null_customer_ids", "—"))
                m5.metric("⚠️ Negative Amounts",   report.get("negative_amounts", "—"))
                m6.metric("🔁 Duplicates",          report.get("duplicate_order_ids", "—"))

                # Step 8: Data quality warning
                issues = (
                    report.get("null_customer_ids", 0)
                    + report.get("negative_amounts", 0)
                    + report.get("duplicate_order_ids", 0)
                )
                if issues > 0:
                    st.warning(f"⚠️ Data quality issues detected — {issues} total anomalies found and corrected.")

                # Step 9: Bedrock analysis
                with st.spinner("🤖 Analysing with AI…"):
                    prompt = (
                        f"You are a data quality analyst. Analyse this pipeline quality report:\n"
                        f"{json.dumps(report, indent=2)}\n\n"
                        f"Respond with exactly two lines:\n"
                        f"Line 1: Status — one of HEALTHY, WARNING, or CRITICAL\n"
                        f"Line 2: One recommendation in max 80 words."
                    )
                    ai_text = bedrock_converse(prompt)

                st.markdown(
                    f'<div class="ai-insight">'
                    f'<div class="ai-label">🤖 AI Analysis — Nova Lite</div>'
                    f'<div class="ai-text">{ai_text}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            except Exception as e:
                st.warning(f"Could not load quality report: {e}")

        elif final_state == "FAILED":
            try:
                run_detail = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)
                err_msg    = run_detail["JobRun"].get("ErrorMessage", "Unknown error")
                st.error(f"❌ Glue job FAILED.\n\n**Error:** {err_msg}")
            except Exception as e:
                err_msg = "Could not retrieve error details."
                st.error(f"❌ Glue job FAILED. {err_msg} ({e})")

            # ── Feature 1: Fire CRITICAL alert on Glue job failure ────────────
            alert_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            dispatched = send_alert(
                channel        = "sns",
                severity       = "CRITICAL",
                subject        = f"[CRITICAL] Glue ETL Failed — {sel_date}",
                job_name       = GLUE_JOB,
                run_id         = run_id,
                date_partition = sel_date,
                reason         = err_msg,
                timestamp      = alert_ts,
            )
            if dispatched:
                st.success("📧 CRITICAL alert dispatched via SNS.")

        else:
            st.warning(f"Job ended with state: {final_state}")

        # ── Feature 1: Fire WARNING alert if DQ issues exceed threshold ───────
        # This block runs after a SUCCEEDED job if the quality report was loaded.
        # 'issues' and 'report' are only in scope when the ETL succeeded and the
        # report was successfully read — guard with hasattr-style check.
        if final_state == "SUCCEEDED":
            try:
                _report_key = f"reports/quality_report_{sel_date}.json"
                _obj        = s3.get_object(Bucket=BUCKET, Key=_report_key)
                _report     = json.loads(_obj["Body"].read().decode("utf-8"))
                _issues     = (
                    _report.get("null_customer_ids",   0)
                    + _report.get("negative_amounts",  0)
                    + _report.get("duplicate_order_ids", 0)
                )
                if _issues > DQ_ISSUE_THRESHOLD:
                    _alert_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    _dispatched = send_alert(
                        channel        = "sns",
                        severity       = "WARNING",
                        subject        = f"[WARNING] Data Quality Issues Detected — {sel_date}",
                        job_name       = GLUE_JOB,
                        run_id         = run_id,
                        date_partition = sel_date,
                        reason         = (
                            f"Total DQ issues ({_issues}) exceeded threshold ({DQ_ISSUE_THRESHOLD}). "
                            f"Null IDs: {_report.get('null_customer_ids', 0)}, "
                            f"Negative amounts: {_report.get('negative_amounts', 0)}, "
                            f"Duplicates: {_report.get('duplicate_order_ids', 0)}."
                        ),
                        timestamp      = _alert_ts,
                    )
                    if _dispatched:
                        st.success("📧 WARNING alert dispatched via SNS — DQ threshold breached.")
            except Exception:
                pass   # quality report already shown above; swallow re-read errors silently


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — Ask Your Data
# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-header">🔍 Ask Your Data</div>', unsafe_allow_html=True)
    st.markdown(
        "Ask questions in plain English — AI converts them to SQL and runs them on Athena.",
    )

    # ── Session state: inject staged quick-question value BEFORE widget renders ──
    if "qq_value" in st.session_state:
        st.session_state["nl_question_input"] = st.session_state.pop("qq_value")

    # ── Feature 2: Restore a history item BEFORE the text-input widget renders ─
    # When the user clicks a history entry its sql/question are staged in
    # session_state["history_restore"] and injected here on the next rerun.
    if "history_restore" in st.session_state:
        _restore = st.session_state.pop("history_restore")
        st.session_state["nl_question_input"] = _restore["question"]
        st.session_state["generated_sql"]     = _restore["sql"]
        st.session_state["athena_result"]      = None   # clear stale results

    # ── Quick question buttons ────────────────────────────────────────────────
    st.markdown('<div style="color:#8b949e;font-size:0.85rem;margin-bottom:8px">💡 Quick Questions</div>',
                unsafe_allow_html=True)
    qq_cols = st.columns(len(QUICK_QUESTIONS))
    for i, qq in enumerate(QUICK_QUESTIONS):
        with qq_cols[i]:
            st.markdown('<div class="qq-btn">', unsafe_allow_html=True)
            if st.button(qq, key=f"qq_{i}"):
                st.session_state["qq_value"] = qq
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── Text input — key= is MANDATORY for persistence ────────────────────────
    user_question = st.text_input(
        "🗣️ Ask your question:",
        placeholder="e.g. What are the top 5 cities by total revenue?",
        key="nl_question_input",
    )

    col_gen, col_run = st.columns([2, 1])

    if col_gen.button("🧠 Generate SQL", key="btn_gen_sql", disabled=not user_question):
        with st.spinner("🤖 Generating SQL with Nova Lite…"):
            prompt = f"""You are an expert AWS Athena SQL engineer.

Database: {ATHENA_DB}

Tables and schemas:
1. {ORDERS_SCHEMA}
2. {CUSTOMERS_SCHEMA}
3. {PRODUCTS_SCHEMA}

Rules:
- For SELECT queries, add LIMIT 100 ONLY if the query does not already have GROUP BY, SUM, COUNT, AVG.
- Aggregation queries (GROUP BY / SUM / COUNT / AVG) — do NOT add LIMIT.
- SHOW / DESCRIBE / DDL statements — NEVER add LIMIT (causes InvalidRequestException).
- Always wrap SUM(amount) or AVG(amount) in CAST(ROUND(...) AS BIGINT) to avoid scientific notation.
- Use fully qualified table names: {ATHENA_DB}.{TBL_ORDERS}, etc.
- Output raw SQL only — no markdown fences, no backticks, no explanation.

User question: {user_question}"""

            try:
                raw_sql = bedrock_converse(prompt)

                # Strip markdown fences if model adds them anyway
                raw_sql = re.sub(r"```sql\s*", "", raw_sql, flags=re.IGNORECASE)
                raw_sql = re.sub(r"```\s*",    "", raw_sql)
                raw_sql = raw_sql.strip()

                # Post-process: strip rogue LIMIT from non-SELECT statements
                first_word = raw_sql.split()[0].upper() if raw_sql.split() else ""
                if first_word != "SELECT":
                    raw_sql = re.sub(r'\bLIMIT\s+\d+\b', '', raw_sql, flags=re.IGNORECASE).strip()

                st.session_state["generated_sql"] = raw_sql
                st.session_state["athena_result"]  = None
                st.session_state["sql_question"]   = user_question

                # ── Feature 2: Persist to query history ──────────────────────
                add_to_query_history(question=user_question, sql=raw_sql)

            except Exception as e:
                st.error(f"❌ Bedrock error: {e}")

    # ── Show generated SQL ────────────────────────────────────────────────────
    if "generated_sql" in st.session_state and st.session_state["generated_sql"]:
        st.markdown('<div style="color:#2DC653;font-weight:600;margin:16px 0 6px">📝 Generated SQL:</div>',
                    unsafe_allow_html=True)
        st.code(st.session_state["generated_sql"], language="sql")

        # ── Run on Athena button ──────────────────────────────────────────────
        if st.button("▶️ Run on Athena", key="btn_run_athena"):
            with st.spinner("⚙️ Executing query on Athena…"):
                try:
                    df_result = run_athena_query(st.session_state["generated_sql"])
                    st.session_state["athena_result"] = df_result
                except Exception as e:
                    st.error(f"❌ Athena error: {e}")
                    st.session_state["athena_result"] = None

    # ── Show Athena result ────────────────────────────────────────────────────
    if "athena_result" in st.session_state and st.session_state["athena_result"] is not None:
        df_res = st.session_state["athena_result"]
        df_display = format_df_currency(df_res)

        st.markdown(
            f'<div style="color:#8b949e;font-size:0.85rem;margin:12px 0 4px">'
            f'Returned <strong style="color:#2DC653">{len(df_res)}</strong> rows</div>',
            unsafe_allow_html=True,
        )
        st.dataframe(df_display, use_container_width=True)

        # Bedrock plain-English explanation
        with st.spinner("🤖 Explaining result…"):
            try:
                explain_prompt = (
                    f"The user asked: '{st.session_state.get('sql_question', '')}'\n"
                    f"SQL result (first 5 rows):\n{df_res.head(5).to_string(index=False)}\n\n"
                    f"Explain this result in exactly one plain-English sentence, "
                    f"suitable for a non-technical business user. No markdown."
                )
                explanation = bedrock_converse(explain_prompt)
                st.markdown(
                    f'<div class="ai-insight">'
                    f'<div class="ai-label">🤖 AI Explanation</div>'
                    f'<div class="ai-text">{explanation}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            except Exception as e:
                st.warning(f"Could not generate explanation: {e}")


    # ── Feature 2: Query History Panel ───────────────────────────────────────
    # Rendered BELOW the main query UI so it never interferes with widget state.
    history = st.session_state.get("query_history", [])
    if history:
        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-header" style="font-size:1rem">🕑 Query History</div>',
            unsafe_allow_html=True,
        )

        # Clear history button — aligned to the right
        _hcol1, _hcol2 = st.columns([6, 1])
        with _hcol2:
            if st.button("🗑️ Clear", key="btn_clear_history"):
                st.session_state["query_history"] = []
                st.rerun()

        # Render each history entry as a compact expandable card
        for idx, entry in enumerate(history):
            # Build a short preview label (first 60 chars of question)
            preview = entry["question"][:60] + ("…" if len(entry["question"]) > 60 else "")
            label   = f"`{entry['timestamp']}`  {preview}"

            with st.expander(label, expanded=False):
                # Show the stored SQL in a code block
                st.code(entry["sql"], language="sql")

                # Restore button — stages values for injection on next rerun
                if st.button(
                    "↩️ Restore this query",
                    key=f"restore_history_{idx}",
                ):
                    st.session_state["history_restore"] = {
                        "question": entry["question"],
                        "sql"     : entry["sql"],
                    }
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — Pipeline Health
# ─────────────────────────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-header">📊 Pipeline Health Dashboard</div>', unsafe_allow_html=True)

    if st.button("🔄 Load Health Dashboard", key="btn_health"):
        with st.spinner("⚙️ Querying Athena for pipeline metrics…"):
            try:
                df_health = run_athena_query(f"""
                    SELECT
                        date,
                        COUNT(*) AS orders,
                        CAST(ROUND(SUM(amount)) AS BIGINT) AS revenue
                    FROM {ATHENA_DB}.{TBL_ORDERS}
                    GROUP BY date
                    ORDER BY date
                """)

                if df_health.empty:
                    st.warning("No data found. Run some ETL jobs first in the Daily Load tab.")
                    st.stop()

                df_health["date"]    = df_health["date"].astype(str)
                df_health["orders"]  = pd.to_numeric(df_health["orders"],  errors="coerce").fillna(0)
                df_health["revenue"] = pd.to_numeric(df_health["revenue"], errors="coerce").fillna(0)

                # ── Summary metrics ───────────────────────────────────────────
                total_orders  = int(df_health["orders"].sum())
                total_revenue = int(df_health["revenue"].sum())
                days_loaded   = len(df_health)

                st.markdown('<div class="section-header" style="font-size:1.1rem">📈 Summary Metrics</div>',
                            unsafe_allow_html=True)
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric("📦 Total Orders",          f"{total_orders:,}")
                mc2.metric("💰 Total Revenue",         f"₹{total_revenue:,}")
                mc3.metric("📅 Days Loaded",           days_loaded)

                st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

                # ── Charts ────────────────────────────────────────────────────
                ch1, ch2 = st.columns(2)
                with ch1:
                    st.markdown('<div class="section-header" style="font-size:1rem">💰 Daily Revenue (₹)</div>',
                                unsafe_allow_html=True)
                    chart_rev = df_health.set_index("date")[["revenue"]]
                    st.bar_chart(chart_rev, color="#2DC653")

                with ch2:
                    st.markdown('<div class="section-header" style="font-size:1rem">📦 Daily Order Volume</div>',
                                unsafe_allow_html=True)
                    chart_ord = df_health.set_index("date")[["orders"]]
                    st.bar_chart(chart_ord, color="#2DC653")

                st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

                # ── Detailed table ────────────────────────────────────────────
                st.markdown('<div class="section-header" style="font-size:1rem">📋 Day-by-Day Breakdown</div>',
                            unsafe_allow_html=True)
                df_display = df_health.copy()
                df_display["revenue"] = df_display["revenue"].apply(lambda x: f"₹{int(x):,}")
                df_display["orders"]  = df_display["orders"].apply(lambda x: f"{int(x):,}")
                df_display.columns    = ["Date", "Orders", "Revenue"]
                st.dataframe(df_display, use_container_width=True, hide_index=True)

                # ── Bedrock executive summary ─────────────────────────────────
                with st.spinner("🤖 Generating executive summary…"):
                    exec_prompt = (
                        f"You are a data analytics director summarising a logistics pipeline.\n"
                        f"Pipeline data (date, orders, revenue):\n"
                        f"{df_health.to_string(index=False)}\n\n"
                        f"Write exactly 3 sentences as an executive summary of pipeline health, "
                        f"trends, and any notable patterns. Use plain English. No markdown."
                    )
                    exec_summary = bedrock_converse(exec_prompt)

                st.markdown(
                    f'<div class="ai-insight">'
                    f'<div class="ai-label">🤖 AI Executive Summary — Nova Lite</div>'
                    f'<div class="ai-text">{exec_summary}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            except Exception as e:
                st.error(f"❌ Health dashboard error: {e}")

    else:
        # Placeholder state when button not yet clicked
        st.markdown(
            '<div style="text-align:center;padding:60px 20px;color:#30363d">'
            '<div style="font-size:4rem">📊</div>'
            '<div style="font-size:1.1rem;margin-top:12px">Click <strong style="color:#2DC653">'
            '🔄 Load Health Dashboard</strong> to view pipeline metrics</div>'
            '</div>',
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — Data Quality Dashboard  (Feature 3)
# Reads quality_report_<date>.json for all loaded days from S3, computes
# multi-dimension DQ scores, and renders charts + an AI narrative.
# All existing tabs and helpers are completely untouched.
# ─────────────────────────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-header">🛡️ Data Quality Dashboard</div>',
                unsafe_allow_html=True)
    st.markdown(
        "Multi-day data quality analysis across all ETL runs — "
        "Completeness · Validity · Uniqueness."
    )

    if st.button("🔍 Scan Quality Reports", key="btn_dq_scan"):

        # ── Load all day reports from S3 ──────────────────────────────────
        dq_records = []
        load_errors = []

        with st.spinner("📥 Reading quality reports from S3…"):
            for _, date_str, _ in DAYS:
                report_key = f"reports/quality_report_{date_str}.json"
                try:
                    obj    = s3.get_object(Bucket=BUCKET, Key=report_key)
                    report = json.loads(obj["Body"].read().decode("utf-8"))
                    dq_records.append(compute_dq_score(report))
                except s3.exceptions.NoSuchKey:
                    load_errors.append(f"`{date_str}` — report not found (ETL not run yet)")
                except Exception as dq_err:
                    load_errors.append(f"`{date_str}` — {str(dq_err)[:80]}")

        if load_errors:
            for msg in load_errors:
                st.warning(f"⚠️ {msg}")

        if not dq_records:
            st.error(
                "❌ No quality reports found. "
                "Run the ETL for at least one day in the **Daily Load** tab first."
            )
            st.stop()

        # ── Build a DataFrame for charts / table ──────────────────────────
        df_dq = pd.DataFrame(dq_records)

        # ── Section B: Overall DQ Score ring ─────────────────────────────
        avg_score       = round(df_dq["overall_score"].mean(), 1)
        avg_complete    = round(df_dq["completeness_pct"].mean(), 1)
        avg_validity    = round(df_dq["validity_pct"].mean(), 1)
        avg_uniqueness  = round(df_dq["uniqueness_pct"].mean(), 1)

        if avg_score >= 90:
            ring_color  = "#2DC653"
            grade_label = "HEALTHY"
            badge_cls   = "dq-badge-healthy"
        elif avg_score >= 70:
            ring_color  = "#ffc400"
            grade_label = "WARNING"
            badge_cls   = "dq-badge-warning"
        else:
            ring_color  = "#f85149"
            grade_label = "CRITICAL"
            badge_cls   = "dq-badge-critical"

        # SVG ring maths: r=62, circumference = 2*pi*62 ≈ 389.6
        CIRC      = 389.6
        dashoffset = round(CIRC * (1 - avg_score / 100), 2)

        st.markdown(
            '<div class="section-header" style="font-size:1.1rem">'
            '📊 Overall Data Quality Score</div>',
            unsafe_allow_html=True,
        )

        score_col, dim_col = st.columns([1, 2])

        with score_col:
            st.markdown(
                f"""
                <div class="dq-score-wrapper">
                  <div class="dq-score-ring">
                    <svg viewBox="0 0 160 160" width="160" height="160">
                      <circle class="ring-bg" cx="80" cy="80" r="62"/>
                      <circle class="ring-fg"
                        cx="80" cy="80" r="62"
                        stroke="{ring_color}"
                        stroke-dasharray="{CIRC}"
                        stroke-dashoffset="{dashoffset}"
                      />
                    </svg>
                    <div class="dq-score-value">
                      {avg_score}<span style="font-size:1rem;color:#8b949e">%</span>
                    </div>
                  </div>
                  <div class="dq-score-label">Overall DQ Score</div>
                  <div class="dq-score-title" style="color:{ring_color}">
                    <span class="dq-badge {badge_cls}">{grade_label}</span>
                  </div>
                  <div style="color:#8b949e;font-size:0.78rem;margin-top:8px">
                    {len(dq_records)} day(s) analysed
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ── Section C: Dimension cards ────────────────────────────────────
        with dim_col:
            st.markdown(
                '<div class="section-header" style="font-size:1rem;margin-top:8px">'
                '🔬 Quality Dimensions</div>',
                unsafe_allow_html=True,
            )
            d1, d2, d3 = st.columns(3)

            def _dim_color(pct: float) -> str:
                if pct >= 95:  return "#2DC653"
                if pct >= 80:  return "#ffc400"
                return "#f85149"

            with d1:
                col = _dim_color(avg_complete)
                st.markdown(
                    f'<div class="dq-dim-card">'
                    f'<div class="dq-dim-icon">📋</div>'
                    f'<div class="dq-dim-name">Completeness</div>'
                    f'<div class="dq-dim-value" style="color:{col}">{avg_complete}%</div>'
                    f'<div class="dq-dim-desc">Valid rows retained after null-ID removal</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with d2:
                col = _dim_color(avg_validity)
                st.markdown(
                    f'<div class="dq-dim-card">'
                    f'<div class="dq-dim-icon">✅</div>'
                    f'<div class="dq-dim-name">Validity</div>'
                    f'<div class="dq-dim-value" style="color:{col}">{avg_validity}%</div>'
                    f'<div class="dq-dim-desc">Rows without negative order amounts</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with d3:
                col = _dim_color(avg_uniqueness)
                st.markdown(
                    f'<div class="dq-dim-card">'
                    f'<div class="dq-dim-icon">🔁</div>'
                    f'<div class="dq-dim-name">Uniqueness</div>'
                    f'<div class="dq-dim-value" style="color:{col}">{avg_uniqueness}%</div>'
                    f'<div class="dq-dim-desc">Rows without duplicate order IDs</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # ── Section D: Multi-day trend charts ─────────────────────────────
        st.markdown(
            '<div class="section-header" style="font-size:1.1rem">'
            '📈 Multi-Day Quality Trends</div>',
            unsafe_allow_html=True,
        )

        chart_df = df_dq.set_index("date")[
            ["overall_score", "completeness_pct", "validity_pct", "uniqueness_pct"]
        ].rename(columns={
            "overall_score"    : "Overall Score",
            "completeness_pct" : "Completeness %",
            "validity_pct"     : "Validity %",
            "uniqueness_pct"   : "Uniqueness %",
        })

        tc1, tc2 = st.columns(2)

        with tc1:
            st.markdown(
                '<div style="color:#2DC653;font-weight:600;font-size:0.92rem;'
                'margin-bottom:6px">🎯 Overall DQ Score per Day (%)</div>',
                unsafe_allow_html=True,
            )
            st.bar_chart(
                chart_df[["Overall Score"]],
                color="#2DC653",
                use_container_width=True,
            )

        with tc2:
            st.markdown(
                '<div style="color:#2DC653;font-weight:600;font-size:0.92rem;'
                'margin-bottom:6px">📉 Issues per Day (raw counts)</div>',
                unsafe_allow_html=True,
            )
            issues_chart = df_dq.set_index("date")[
                ["null_ids", "neg_amounts", "dup_ids"]
            ].rename(columns={
                "null_ids"    : "Null IDs",
                "neg_amounts" : "Negative Amounts",
                "dup_ids"     : "Duplicate IDs",
            })
            st.bar_chart(issues_chart, use_container_width=True)

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # ── Section E: Per-day issues table ──────────────────────────────
        st.markdown(
            '<div class="section-header" style="font-size:1.1rem">'
            '📋 Day-by-Day Quality Breakdown</div>',
            unsafe_allow_html=True,
        )

        def _badge_html(grade: str) -> str:
            cls = {
                "HEALTHY" : "dq-badge-healthy",
                "WARNING" : "dq-badge-warning",
                "CRITICAL": "dq-badge-critical",
            }.get(grade, "dq-badge-warning")
            icon = {"HEALTHY": "✅", "WARNING": "⚠️", "CRITICAL": "🚨"}.get(grade, "⚠️")
            return f'<span class="dq-badge {cls}">{icon} {grade}</span>'

        rows_html = ""
        for rec in dq_records:
            rows_html += (
                f"<tr>"
                f"<td><strong>{rec['date']}</strong></td>"
                f"<td>{rec['input_rows']:,}</td>"
                f"<td>{rec['output_rows']:,}</td>"
                f"<td style='color:#f85149'>{rec['null_ids']}</td>"
                f"<td style='color:#ffc400'>{rec['neg_amounts']}</td>"
                f"<td style='color:#ffc400'>{rec['dup_ids']}</td>"
                f"<td>{rec['rows_dropped']:,}</td>"
                f"<td><strong style='color:#e6edf3'>{rec['overall_score']}%</strong></td>"
                f"<td>{_badge_html(rec['grade'])}</td>"
                f"</tr>"
            )

        table_html = f"""
        <table class="dq-issues-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Input Rows</th>
              <th>Output Rows</th>
              <th>🚫 Null IDs</th>
              <th>⚠️ Neg. Amounts</th>
              <th>🔁 Duplicates</th>
              <th>Rows Dropped</th>
              <th>DQ Score</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows_html}
          </tbody>
        </table>
        """
        st.markdown(table_html, unsafe_allow_html=True)

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

        # ── Section F: Bedrock AI narrative ──────────────────────────────
        with st.spinner("🤖 Generating Data Quality narrative with AI…"):
            try:
                dq_summary_rows = [
                    f"  Date={r['date']}: score={r['overall_score']}%, "
                    f"completeness={r['completeness_pct']}%, "
                    f"validity={r['validity_pct']}%, "
                    f"uniqueness={r['uniqueness_pct']}%, "
                    f"nulls={r['null_ids']}, negatives={r['neg_amounts']}, "
                    f"duplicates={r['dup_ids']}, dropped={r['rows_dropped']}"
                    for r in dq_records
                ]
                dq_prompt = (
                    f"You are a senior data quality engineer reviewing a logistics pipeline.\n"
                    f"Multi-day quality summary ({len(dq_records)} day(s)):\n"
                    + "\n".join(dq_summary_rows)
                    + "\n\nRespond with exactly 3 concise sentences:\n"
                    f"Sentence 1: Overall data quality health status and trend.\n"
                    f"Sentence 2: The most critical issue dimension and which day it peaked.\n"
                    f"Sentence 3: One actionable remediation recommendation for the data team.\n"
                    f"Use plain English. No markdown formatting."
                )
                dq_ai_text = bedrock_converse(dq_prompt)

                st.markdown(
                    f'<div class="ai-insight">'
                    f'<div class="ai-label">🤖 AI Data Quality Analysis — Nova Lite</div>'
                    f'<div class="ai-text">{dq_ai_text}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            except Exception as ai_err:
                st.warning(f"Could not generate AI narrative: {ai_err}")

    else:
        # ── Placeholder when button not yet clicked ───────────────────────
        st.markdown(
            '<div style="text-align:center;padding:60px 20px;color:#30363d">'
            '<div style="font-size:4rem">🛡️</div>'
            '<div style="font-size:1.1rem;margin-top:12px">Click '
            '<strong style="color:#2DC653">🔍 Scan Quality Reports</strong> '
            'to analyse data quality across all loaded days</div>'
            '<div style="font-size:0.85rem;margin-top:8px;color:#21262d">'
            'Requires at least one completed ETL run in the Daily Load tab</div>'
            '</div>',
            unsafe_allow_html=True,
        )


# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown(
    '<div style="text-align:center;color:#30363d;font-size:0.8rem;padding:8px 0">'
    '⚡ Sigma Matrix · AI Data Pipeline Dashboard · '
    'AWS S3 · Glue · Athena · Bedrock (Nova Lite) · '
    'Built with Streamlit'
    '</div>',
    unsafe_allow_html=True,
)
