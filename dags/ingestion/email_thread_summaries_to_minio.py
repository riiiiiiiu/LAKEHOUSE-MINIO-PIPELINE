import os
import logging
from datetime import datetime, date,timedelta
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from airflow import DAG 
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)
# ── Paths ────────────────────────────────────────────────────────────────────
DATASET_NAME = "email_thread_summaries"
logging.basicConfig(level=logging.INFO)

BASE_DIR    = "/opt/airflow"
CSV_PATH    = f"{BASE_DIR}/dags/extraction/{DATASET_NAME}.csv"
SCHEMA_PATH = f"{BASE_DIR}/dags/ingestion/{DATASET_NAME}/config/schema_expected.yaml"
PARQUET_DIR = "/tmp/email_thread_summaries"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",     "lakehouse")
MINIO_PREFIX   = "email_thread_summaries"
MINIO_SECURE   = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ── YAML schema type → pandas dtype ──────────────────────────────────────────
DTYPE_MAP = {
    "integer":   "Int64",
    "float":     "float64",
    "text":      "object",
    "varchar":   "object",
    "boolean":   "boolean",
    "date":      "object",
    "timestamp": "object",
}

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "Data Team",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task 1: file_check ────────────────────────────────────────────────────────
def file_check():
    # BUG FIX: logging.logging.logging.log.info → logging.logging.info
    for p in [CSV_PATH, SCHEMA_PATH]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing required file: {p}")
    logging.info("All required files exist.")


# ── Task 2: validate_schema ───────────────────────────────────────────────────
def validate_schema():
    # BUG FIX: logging.logging.logging.log.info → logging.logging.info
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_cols = [col["name"] for col in schema["columns"]]

    df_header  = pd.read_csv(CSV_PATH, nrows=0)
    actual_cols = df_header.columns.tolist()

    missing = set(expected_cols) - set(actual_cols)
    extra   = set(actual_cols)   - set(expected_cols)

    errors = []
    if missing:
        errors.append(f"Missing columns in CSV: {sorted(missing)}")
    if extra:
        errors.append(f"Unexpected columns in CSV: {sorted(extra)}")
    if errors:
        raise ValueError("Schema mismatch:\n  " + "\n  ".join(errors))

    logging.info("Schema validation passed — columns: %s", expected_cols)


# ── Task 3: transformation ────────────────────────────────────────────────────
def transformation(**context):
    # BUG FIX 1: for p in (SCHEMA_PATH) iterates over characters of a string
    #            → use with open() directly
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    df = pd.read_csv(CSV_PATH)
    logging.info("Read %d rows from CSV", len(df))

    for col_def in schema["columns"]:
        col      = col_def["name"]
        dtype    = DTYPE_MAP.get(col_def["type"], "object")
        nullable = col_def.get("nullable", True)

        # BUG FIX 2: str().strip() → str.strip()
        if dtype == "object":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", pd.NA)

        # BUG FIX 3: wrong null check logging.logic
        # "if null_count and nullable > 0" → "if not nullable and null_count > 0"
        null_count = df[col].isna().sum()
        if not nullable and null_count > 0:
            raise ValueError(
                f"Column '{col}' is NOT NULL but contains {null_count} null value(s)."
            )

        # BUG FIX 4: indentation — try/except must be inside the for loop
        try:
            df[col] = df[col].astype(dtype)
        except (ValueError, TypeError) as exc:
            raise TypeError(
                f"Cannot cast column '{col}' to '{dtype}': {exc}"
            ) from exc

    # Inject partition column
    df["load_date"] = date.today().isoformat()

    tmp_path = f"/tmp/email_thread_summaries_transformed_{date.today().isoformat()}.json"
    df.to_json(tmp_path, orient="records", force_ascii=False)

    # BUG FIX 5: context is not a variable — function must accept **context
    context["ti"].xcom_push(key="transformed_path", value=tmp_path)

    logging.info("Transform complete — %d rows written to %s", len(df), tmp_path)


def convert_to_parquet(**context) -> None:
    tmp_path =context["ti"].xcom_pull(key="transformed_path", task_ids="transformation")
    if not tmp_path or not os.path.exists(tmp_path):
        raise FileNotFoundError(f"Transformed file not found: {tmp_path}")
    
    df = pd.read_json(tmp_path,orient="records")

    df["thread_id"] = df["thread_id"].astype("Int64")
    df["summary"] = df["summary"].astype("string")

    os.makedirs("/tmp/parquet", exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=PARQUET_DIR,
        partition_cols=["load_date"],
        compression="snappy",
        #existing_data_behavior="overwrite_or_ignore",
    )
    logging.info("Parquet dataset written to %s", PARQUET_DIR)


def upload_to_minio() -> None:
    """
    Walk PARQUET_DIR and upload every .parquet file to MinIO.

    Object path: <MINIO_PREFIX>/load_date=YYYY-MM-DD/<filename>.parquet
    Creates the bucket automatically if it doesn't exist.
    Raises RuntimeError if zero files are found to upload.
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )

    # Auto-create bucket
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logging.info("Bucket created: %s", MINIO_BUCKET)
        else:
            logging.info("Bucket already exists: %s", MINIO_BUCKET)
    except S3Error as exc:
        raise RuntimeError(f"MinIO bucket check/create failed: {exc}") from exc

    # Upload all parquet files
    uploaded = 0
    for root, _dirs, files in os.walk(PARQUET_DIR):
        for file in sorted(files):
            if not file.endswith(".parquet"):
                continue
            local_path  = os.path.join(root, file)
            relative    = os.path.relpath(local_path, PARQUET_DIR)
            object_name = f"{MINIO_PREFIX}/{relative}".replace("\\", "/")

            try:
                client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    local_path,
                    content_type="application/octet-stream",
                )
                logging.info("Uploaded: s3://%s/%s", MINIO_BUCKET, object_name)
                uploaded += 1
            except S3Error as exc:
                raise RuntimeError(
                    f"Failed to upload {object_name}: {exc}"
                ) from exc

    if uploaded == 0:
        raise RuntimeError(
            f"No .parquet files found under {PARQUET_DIR}. "
            "Check if the convert_to_parquet task ran successfully."
        )

    logging.info("Upload complete — %d file(s) uploaded to s3://%s/%s/",
             uploaded, MINIO_BUCKET, MINIO_PREFIX)

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="email_thread_summaries_to_minio",
    description="Ingest email_thread_summaries CSV → validate → transform → MinIO",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "minio", "email"],
    default_args=default_args,
) as dag:

    t1 = PythonOperator(
        task_id="file_check",
        python_callable=file_check,
    )

    t2 = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t3 = PythonOperator(
        task_id="transformation",
        python_callable=transformation,
        provide_context=True,   # required for **context to receive ti
    )

    t4 = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
        provide_context=True,
    )
    t5 = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )
    t1 >> t2 >> t3 >> t4 >> t5