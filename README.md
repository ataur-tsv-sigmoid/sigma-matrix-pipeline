# ⚡ Sigma Matrix — AI-Powered Data Pipeline Dashboard

---

## 👥 Team

| Name

| **Ata Ur Rehman** 
| **Sharoz Farhan Afridi** 
| **Sneha Sahu**

> **Company:** Sigma TechZone · Logistics Division  
> **Repository:** [ataur-tsv-sigmoid/sigma-matrix-pipeline](https://github.com/ataur-tsv-sigmoid/sigma-matrix-pipeline)

---

## 📌 Project Overview

**Sigma Matrix** is a production-grade, AI-powered data pipeline dashboard built for a logistics company. It automates the full lifecycle of daily order data — from raw ingestion to quality validation, Athena querying, and AI-driven insights — all through a single Streamlit browser interface.

No SQL knowledge. No manual cleaning. No waiting for a data analyst.

---

## 🏗️ Architecture

```
Raw CSVs (S3)
    │
    ▼
AWS Glue Python Shell Job  ──► Cleaned CSVs + Quality Report (S3)
    │
    ▼
Amazon Athena  ──► SQL Query Engine on processed S3 data
    │
    ▼
Amazon Bedrock (Nova Lite)  ──► NL→SQL · Quality Analysis · Executive Summary
    │
    ▼
Streamlit Dashboard  ──► 4-tab browser UI
    │
    ▼
AWS SNS  ──► Email Alerts (on ETL failure or DQ threshold breach)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | Streamlit (wide layout, dark theme) |
| ETL Engine | AWS Glue Python Shell (GlueVersion 1.0) |
| Storage | Amazon S3 |
| Query Engine | Amazon Athena |
| AI / LLM | Amazon Bedrock — `us.amazon.nova-lite-v1:0` |
| Alerting | AWS SNS (email subscriptions) |
| Language | Python 3.10+ |
| Key Libraries | `boto3`, `pandas`, `streamlit` |
| Region | `us-east-1` |

---

## ☁️ AWS Resource Names

| Resource | Name |
|----------|------|
| S3 Bucket | `sigma-matrix-bucket` |
| Glue Job | `sigma-matrix-etl` |
| Athena Database | `sigmamatrixdb` |
| Orders Table | `sigmamatrixorders` |
| Customers Table | `sigmamatrixcustomers` |
| Products Table | `sigmamatrixproducts` |
| Glue IAM Role | `SigmaGlueServiceRole` |
| SNS Topic | `sigma-matrix-alerts` |

---

## 📁 Project Structure

```
sigma-matrix-pipeline/
├── app.py                        # Main Streamlit dashboard
├── requirements.txt              # Python dependencies
├── .streamlit/
│   └── secrets.toml              # SNS ARN and future Slack config
├── glue_scripts/
│   └── etl.py                    # AWS Glue Python Shell ETL job
└── data/
    ├── orders_day1.csv           # Raw orders — Day 1 (2026-05-01)
    ├── orders_day2.csv           # Raw orders — Day 2 (2026-05-02)
    ├── orders_day3.csv           # Raw orders — Day 3 (2026-05-03)
    ├── orders_day4.csv           # Raw orders — Day 4 (2026-05-04)
    ├── orders_day5.csv           # Raw orders — Day 5 (2026-05-05)
    ├── customers.csv             # Reference — customer master
    └── products.csv              # Reference — product catalogue
```

### S3 Bucket Layout

```
sigma-matrix-bucket/
├── raw/
│   ├── orders/date=YYYY-MM-DD/orders.csv
│   ├── customers.csv
│   └── products.csv
├── processed/
│   ├── orders/date=YYYY-MM-DD/orders.csv
│   ├── customers/customers.csv
│   └── products/products.csv
├── reports/
│   └── quality_report_YYYY-MM-DD.json
├── glue-scripts/
│   └── etl.py
└── athena-results/
```

---

## 📊 Database Schema

### Orders Table — `sigmamatrixorders`
| Column | Type | Notes |
|--------|------|-------|
| order_id | STRING | |
| customer_id | STRING | |
| product_id | STRING | |
| quantity | INT | |
| amount | DOUBLE | |
| status | STRING | |
| payment_method | STRING | |
| city | STRING | |
| created_at | STRING | |
| processed_at | STRING | Added by ETL |
| is_high_value | STRING | `True` if amount > 10,000 |
| date | STRING | Partition key |

### Customers Table — `sigmamatrixcustomers`
`customer_id, name, email, phone, city, tier, signup_date`

### Products Table — `sigmamatrixproducts`
`product_id, name, category, price, stock_quantity, is_active`

---

## 🖥️ Dashboard Features

### Tab 1 — 🔧 Setup Pipeline

One-click infrastructure deployment across **8 steps**:

1. Create S3 bucket (`us-east-1`, no CreateBucketConfiguration)
2. Upload `glue_scripts/etl.py` → `s3://sigma-matrix-bucket/glue-scripts/etl.py`
3. Upload `customers.csv` and `products.csv` to both `raw/` and `processed/`
4. Delete + recreate Glue Python Shell job (GlueVersion `1.0`, MaxCapacity `0.0625`)
5. Create Athena database: `CREATE DATABASE IF NOT EXISTS sigmamatrixdb`
6. Create orders table with `IF NOT EXISTS` and partition support
7. Drop + recreate customers table (ensures fresh schema)
8. Drop + recreate products table (ensures fresh schema)

Each step displays a live ✅ / ❌ status card. Fully **idempotent** — safe to run multiple times.

---

### Tab 2 — 📦 Daily Load

Runs the full daily ETL cycle for a selected day (Day 1–5):

1. **Upload** raw orders CSV → `s3://sigma-matrix-bucket/raw/orders/date=YYYY-MM-DD/orders.csv`
2. **Trigger** AWS Glue job with `--job_type orders`, `--bucket_name`, `--date_partition`
3. **Poll** job every 3 seconds (max 40 polls) with a live progress bar
4. On `FAILED` — display `ErrorMessage` from `get_job_run()` + fire **CRITICAL SNS alert**
5. On `SUCCEEDED` — run `MSCK REPAIR TABLE` to register new Athena partitions
6. **Read** quality report JSON from S3 and display as 6 metric cards:
   - Input Rows · Output Rows · Rows Dropped · Null Customer IDs · Negative Amounts · Duplicates
7. If any DQ issue detected → `⚠️ Data quality issues detected` warning
8. **Bedrock AI verdict** — `HEALTHY / WARNING / CRITICAL` + one recommendation
9. If total DQ issues > threshold (default: 10) → fire **WARNING SNS alert**

---

### Tab 3 — 🔍 Ask Your Data

Natural language querying powered by Bedrock Nova Lite:

- **5 Quick Question buttons** (logistics domain):
  - Top 5 cities by revenue
  - Daily order trend
  - High value orders per day
  - Top 3 payment methods by order count
  - Average order amount by city
- **Free-text input** — ask any question in plain English
- Bedrock generates **valid Athena SQL** with:
  - Correct fully qualified table names
  - `CAST(ROUND(SUM(amount)) AS BIGINT)` to avoid scientific notation
  - `LIMIT 100` only on simple SELECT (not aggregations or DDL)
  - Rogue `LIMIT` stripped from non-SELECT statements via `re.sub`
- SQL displayed in a code block before execution
- **Run on Athena** button executes the query
- All monetary columns formatted as **₹X,XX,XXX** (Indian Rupees, no decimals)
- Bedrock explains the result in **one plain-English sentence**

#### Query History (Feature 2)
- Automatically saves every generated query (question + SQL + timestamp)
- Keeps the **latest 10 queries** (FIFO rolling window)
- Each entry shown as a collapsible card with timestamp preview
- **↩️ Restore** button repopulates the question input and SQL
- **🗑️ Clear** button wipes the history
- Fully session-state safe — uses staging pattern to avoid `StreamlitAPIException`

---

### Tab 4 — 📊 Pipeline Health

Full pipeline health overview on demand:

- Queries Athena: `SELECT date, COUNT(*), CAST(ROUND(SUM(amount)) AS BIGINT) AS revenue GROUP BY date`
- **Bar chart** — Daily Revenue (₹)
- **Bar chart** — Daily Order Volume
- **3 metric cards** — Total Orders · Total Revenue · Days Loaded
- **Day-by-day breakdown** table
- **Bedrock 3-sentence executive summary** — trend analysis, peak day, anomaly flags

---

## 🔔 Email Alerting — AWS SNS (Feature 1)

### When emails are sent

| Trigger | Severity | Condition |
|---------|----------|-----------|
| Glue job `FAILED` | `CRITICAL` | Always, immediately on failure |
| DQ threshold breach | `WARNING` | `null_ids + negatives + duplicates > DQ_ISSUE_THRESHOLD` |

### Alert email contents
- Severity level · Job name · Run ID · Date partition · Timestamp · Reason

### Architecture — modular by design
`send_alert()` is the single dispatch point. Adding Slack later requires only appending a `requests.post()` call inside that function — no callers change.

### SNS Setup

```bash
# 1. Create the SNS topic
aws sns create-topic --name sigma-matrix-alerts --region us-east-1

# 2. Subscribe a recipient email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:sigma-matrix-alerts \
  --protocol email \
  --notification-endpoint your@email.com

# 3. Recipient must click "Confirm subscription" in the confirmation email
```

### Required IAM permissions (add to the role running the app)
```json
{
  "Effect": "Allow",
  "Action": ["sns:Publish"],
  "Resource": "arn:aws:sns:us-east-1:ACCOUNT_ID:sigma-matrix-alerts"
}
```

---

## ⚙️ Glue ETL Job — `glue_scripts/etl.py`

### Arguments
| Argument | Description |
|----------|-------------|
| `--bucket_name` | S3 bucket name |
| `--date_partition` | Date string e.g. `2026-05-01` |
| `--job_type` | `orders` or `reference` |

### `orders` job flow
1. Read `raw/orders/date={date}/orders.csv`
2. Audit: count null `customer_id`, negative `amount`, duplicate `order_id`
3. Fix: `dropna`, `abs()`, `drop_duplicates(keep="first")`
4. Enrich: `processed_at` (UTC ISO string), `is_high_value` (bool as string)
5. Write cleaned CSV → `processed/orders/date={date}/orders.csv`
6. Write quality report JSON → `reports/quality_report_{date}.json`

### `reference` job flow
- Copies `customers.csv` and `products.csv` from `raw/` → `processed/` using `s3.copy_object()`

### Quality report schema
```json
{
  "date": "2026-05-01",
  "input_rows": 500,
  "output_rows": 487,
  "null_customer_ids": 5,
  "negative_amounts": 3,
  "duplicate_order_ids": 5,
  "rows_dropped": 13,
  "status": "SUCCESS"
}
```

---

## 🚀 Getting Started

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS credentials
```bash
aws configure
# Region: us-east-1
```

### 3. Configure secrets
Edit `.streamlit/secrets.toml`:
```toml
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:sigma-matrix-alerts"
```

### 4. Run the app
```bash
streamlit run app.py
```

### 5. First-time setup
- Open **Tab 1 — 🔧 Setup Pipeline**
- Click **🚀 Deploy Pipeline**
- Wait for all 8 steps to show ✅

### 6. Load daily data
- Open **Tab 2 — 📦 Daily Load**
- Select a day from the dropdown
- Click **▶️ Run ETL**

---

## 📦 Requirements

```
boto3==1.38.17
pandas==2.2.3
streamlit==1.45.1
```

---

## 🔒 IAM Permissions Required

The AWS principal (user or role) running the app needs:

```
s3:CreateBucket, s3:PutObject, s3:GetObject, s3:ListBucket
glue:CreateJob, glue:DeleteJob, glue:StartJobRun, glue:GetJobRun, glue:ListJobs
athena:StartQueryExecution, athena:GetQueryExecution, athena:GetQueryResults
bedrock:InvokeModel (or bedrock:Converse)
sns:Publish
```

The Glue job itself runs under `SigmaGlueServiceRole`, which needs:
```
s3:GetObject, s3:PutObject  (on sigma-matrix-bucket)
logs:CreateLogGroup, logs:PutLogEvents
```

---

## 🐛 Known Issues Handled

| Issue | Resolution |
|-------|------------|
| GlueVersion `3.0` fails for Python Shell | Always use `"1.0"` |
| Athena header row detection | Dynamic compare — not blind `Rows[0]` skip |
| `LIMIT` on SHOW/DDL breaks Athena | Stripped via `re.sub` post-processing |
| Streamlit session state `StreamlitAPIException` | Staging pattern with `qq_value` / `history_restore` |
| Scientific notation in Athena results | `CAST(ROUND(SUM(amount)) AS BIGINT)` |
| Stale Glue config | Always `delete_job` → `create_job` |
| Dim tables not updating | `DROP TABLE IF EXISTS` → `CREATE` |
| S3 bytes encoding | `.encode("utf-8")` not `BytesIO()` |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push and open a Pull Request

---

*Built with ❤️ by Team Sigma Matrix · Sigma TechZone Logistics · 2026*
