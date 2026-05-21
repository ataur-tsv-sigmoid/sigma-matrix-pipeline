"""
AWS Glue Python Shell ETL Script
Company  : TechZone Ltd
Author   : Senior Data Engineer
Purpose  : Process orders and reference data from S3 raw to processed layer
           with data quality checks and reporting.

Usage (Glue Job parameters):
    --bucket_name      S3 bucket name (without s3:// prefix)
    --date_partition   Partition date string, e.g. 2024-01-15
    --job_type         Either "orders" or "reference"
"""

import sys
import json
import logging
import io
from datetime import datetime, timezone

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Argument parsing (sys.argv — Python Shell, no argparse dependency needed)
# ---------------------------------------------------------------------------
def parse_args(argv):
    """Parse --key value pairs from sys.argv into a dict."""
    args = {}
    it = iter(argv[1:])
    for token in it:
        if token.startswith("--"):
            key = token.lstrip("--")
            try:
                value = next(it)
            except StopIteration:
                raise ValueError(f"Argument '{token}' has no value.")
            args[key] = value
    return args


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def s3_read_csv(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Download a CSV from S3 and return as a DataFrame."""
    logger.info("Reading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(content))
    logger.info("Read %d rows, %d columns from s3://%s/%s", len(df), len(df.columns), bucket, key)
    return df


def s3_write_csv(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload a DataFrame as CSV to S3."""
    logger.info("Writing %d rows to s3://%s/%s", len(df), bucket, key)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info("Successfully wrote CSV to s3://%s/%s", bucket, key)


def s3_write_json(s3_client, data: dict, bucket: str, key: str) -> None:
    """Upload a dict as a JSON file to S3."""
    logger.info("Writing JSON report to s3://%s/%s", bucket, key)
    body = json.dumps(data, indent=2, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    logger.info("Successfully wrote JSON report to s3://%s/%s", bucket, key)


def s3_copy_object(s3_client, bucket: str, src_key: str, dst_key: str) -> None:
    """Copy an S3 object within the same bucket."""
    copy_source = {"Bucket": bucket, "Key": src_key}
    logger.info("Copying s3://%s/%s → s3://%s/%s", bucket, src_key, bucket, dst_key)
    s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=dst_key)
    logger.info("Copy complete: s3://%s/%s", bucket, dst_key)


# ---------------------------------------------------------------------------
# Job: orders
# ---------------------------------------------------------------------------
def run_orders_job(s3_client, bucket: str, date_partition: str) -> None:
    """
    ETL pipeline for the orders dataset:
      1. Read raw CSV
      2. Audit data quality issues
      3. Apply fixes
      4. Enrich with derived columns
      5. Write processed CSV
      6. Write quality report JSON
    """
    raw_key       = f"raw/orders/date={date_partition}/orders.csv"
    processed_key = f"processed/orders/date={date_partition}/orders.csv"
    report_key    = f"reports/quality_report_{date_partition}.json"

    # ------------------------------------------------------------------
    # Step 1: Read raw data
    # ------------------------------------------------------------------
    logger.info("Step 1/6 — Reading raw orders data.")
    df = s3_read_csv(s3_client, bucket, raw_key)
    input_rows = len(df)
    logger.info("Input row count: %d", input_rows)

    # ------------------------------------------------------------------
    # Step 2: Data quality audit
    # ------------------------------------------------------------------
    logger.info("Step 2/6 — Auditing data quality.")

    # Null customer_ids
    null_customer_ids = int(df["customer_id"].isna().sum())
    logger.info("Null customer_ids found    : %d", null_customer_ids)

    # Negative amounts
    negative_amounts = int((df["amount"] < 0).sum())
    logger.info("Negative amount rows found : %d", negative_amounts)

    # Duplicate order_ids
    duplicate_order_ids = int(df.duplicated(subset=["order_id"]).sum())
    logger.info("Duplicate order_id rows    : %d", duplicate_order_ids)

    # ------------------------------------------------------------------
    # Step 3: Apply fixes
    # ------------------------------------------------------------------
    logger.info("Step 3/6 — Applying data fixes.")

    # Fix 1: Drop rows where customer_id is null
    df = df.dropna(subset=["customer_id"])
    logger.info("After dropping null customer_ids: %d rows remaining", len(df))

    # Fix 2: Replace negative amounts with their absolute values
    df["amount"] = df["amount"].abs()
    logger.info("Negative amounts corrected to absolute values.")

    # Fix 3: Drop duplicate order_ids, keeping first occurrence
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    logger.info("After deduplication on order_id: %d rows remaining", len(df))

    output_rows  = len(df)
    rows_dropped = input_rows - output_rows
    logger.info("Rows dropped in total: %d", rows_dropped)

    # ------------------------------------------------------------------
    # Step 4: Enrich with derived columns
    # ------------------------------------------------------------------
    logger.info("Step 4/6 — Adding derived columns.")

    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["processed_at"]  = processed_at
    df["is_high_value"] = df["amount"].apply(lambda x: True if x > 10000 else False)

    logger.info("Added 'processed_at' = %s", processed_at)
    logger.info("Added 'is_high_value' column (True where amount > 10000).")

    # ------------------------------------------------------------------
    # Step 5: Write processed CSV to S3
    # ------------------------------------------------------------------
    logger.info("Step 5/6 — Writing processed orders CSV.")
    s3_write_csv(s3_client, df, bucket, processed_key)

    # ------------------------------------------------------------------
    # Step 6: Write quality report JSON to S3
    # ------------------------------------------------------------------
    logger.info("Step 6/6 — Writing quality report JSON.")

    quality_report = {
        "date"               : date_partition,
        "input_rows"         : input_rows,
        "output_rows"        : output_rows,
        "null_customer_ids"  : null_customer_ids,
        "negative_amounts"   : negative_amounts,
        "duplicate_order_ids": duplicate_order_ids,
        "rows_dropped"       : rows_dropped,
        "status"             : "SUCCESS",
    }
    s3_write_json(s3_client, quality_report, bucket, report_key)

    logger.info("Orders job completed successfully.")
    logger.info("Quality report summary: %s", json.dumps(quality_report))


# ---------------------------------------------------------------------------
# Job: reference
# ---------------------------------------------------------------------------
def run_reference_job(s3_client, bucket: str) -> None:
    """
    Copy reference files (customers.csv, products.csv) from raw/ to processed/
    without any transformation.
    """
    reference_files = ["customers.csv", "products.csv"]

    logger.info("Step 1/1 — Copying reference files from raw/ to processed/.")
    for filename in reference_files:
        src_key = f"raw/{filename}"
        dst_key = f"processed/{filename}"
        s3_copy_object(s3_client, bucket, src_key, dst_key)

    logger.info("Reference job completed successfully. Files copied: %s", reference_files)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main():
    try:
        # ----------------------------------------------------------------
        # Parse arguments
        # ----------------------------------------------------------------
        logger.info("Parsing job arguments from sys.argv.")
        args = parse_args(sys.argv)

        bucket_name     = args.get("bucket_name")
        date_partition  = args.get("date_partition")
        job_type        = args.get("job_type")

        if not bucket_name:
            raise ValueError("Missing required argument: --bucket_name")
        if not job_type:
            raise ValueError("Missing required argument: --job_type")

        logger.info("Job arguments resolved:")
        logger.info("  bucket_name    = %s", bucket_name)
        logger.info("  date_partition = %s", date_partition)
        logger.info("  job_type       = %s", job_type)

        # ----------------------------------------------------------------
        # Initialise boto3 S3 client
        # ----------------------------------------------------------------
        logger.info("Initialising boto3 S3 client.")
        s3_client = boto3.client("s3")

        # ----------------------------------------------------------------
        # Route to the appropriate job handler
        # ----------------------------------------------------------------
        if job_type == "orders":
            if not date_partition:
                raise ValueError("Missing required argument for orders job: --date_partition")
            logger.info("Starting ORDERS ETL job for date partition: %s", date_partition)
            run_orders_job(s3_client, bucket_name, date_partition)

        elif job_type == "reference":
            logger.info("Starting REFERENCE copy job.")
            run_reference_job(s3_client, bucket_name)

        else:
            raise ValueError(
                f"Unknown job_type '{job_type}'. Expected one of: 'orders', 'reference'."
            )

        logger.info("ETL job '%s' finished successfully.", job_type)

    except Exception as exc:
        logger.error("ETL job failed with exception: %s", exc, exc_info=True)
        raise


if __name__ == "__main__":
    main()
