# Email Thread Summaries – Automated Data Ingestion Pipeline

## Project Overview

This project implements an automated ETL (Extract, Transform, Load) pipeline using **Apache Airflow, Python, Pandas, Apache Parquet, and MinIO**.

The pipeline extracts email thread summary data from a CSV file, validates the incoming data against a predefined YAML schema, performs data type conversions and null-value checks, converts the transformed data into partitioned Parquet files, and finally uploads the files to MinIO object storage.

The workflow is orchestrated using Apache Airflow and is configured to execute daily.

## Architecture

```text
       CSV Source Data
              |
              v
        File Validation
              |
              v
        Schema Validation
         (YAML Schema)
              |
              v
       Data Transformation
       - Type Casting
       - Null Validation
       - Data Cleaning
       - Add Load Date
              |
              v
       Parquet Conversion
       - Apache Arrow
       - Snappy Compression
       - Date Partitioning
              |
              v
        MinIO Object Storage
         (Data Lakehouse)
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Pipeline development |
| Apache Airflow | Workflow orchestration |
| Pandas | Data extraction and transformation |
| PyYAML | Schema configuration and validation |
| Apache PyArrow | Parquet conversion |
| Apache Parquet | Columnar data storage |
| MinIO | S3-compatible object storage |
| Docker | Containerized environment (if configured) |
| Git & GitHub | Version control and project hosting |

## Pipeline Workflow

The Airflow DAG is named:

`email_thread_summaries_to_minio`

It consists of five tasks.

### 1. File Check

- Checks whether the source CSV file exists.
- Checks whether the expected YAML schema file exists.
- Raises a `FileNotFoundError` if either file is missing.

### 2. Schema Validation

- Reads the expected schema from the YAML configuration file.
- Compares expected column names against actual CSV columns.
- Identifies missing and unexpected columns.
- Stops the pipeline if a schema mismatch is detected.

### 3. Data Transformation

- Reads the source CSV using Pandas.
- Applies data types based on the YAML schema.
- Removes leading and trailing whitespace from text fields.
- Validates NULL values against column nullability rules.
- Raises errors for invalid data type conversions.
- Adds a `load_date` column for data partitioning.
- Saves transformed data as a temporary JSON file.
- Passes the temporary file path to the next Airflow task using XCom.

### 4. Parquet Conversion

- Reads the transformed JSON data.
- Converts the data into an Apache Arrow table.
- Writes the output in Apache Parquet format.
- Uses Snappy compression.
- Partitions the dataset using the `load_date` column.

### 5. Upload to MinIO

- Connects to MinIO using environment-based configuration.
- Checks whether the destination bucket exists.
- Creates the bucket automatically if it does not exist.
- Uploads all generated Parquet files.
- Preserves the partition directory structure.
- Raises an error if no Parquet files are available for upload.

## MinIO Storage Structure

The generated files are stored in the MinIO bucket using the following structure:

```text
lakehouse/
└── email_thread_summaries/
    └── load_date=YYYY-MM-DD/
        └── part-....parquet
```

Example:

```text
lakehouse/
└── email_thread_summaries/
    └── load_date=2026-11-13/
        └── part-0.parquet
```

## Project Configuration

The pipeline uses environment variables to configure MinIO.

| Variable | Description | Default |
|---|---|---|
| MINIO_ENDPOINT | MinIO server endpoint | minio:9000 |
| MINIO_ACCESS_KEY | MinIO access key | minioadmin |
| MINIO_SECRET_KEY | MinIO secret key | minioadmin123 |
| MINIO_BUCKET | Destination bucket | lakehouse |
| MINIO_SECURE | Enable HTTPS connection | false |

**Security note:** The defaults shown above are for local development only. Use secure credentials in production and never commit real credentials to GitHub.

## Airflow Scheduling

The DAG is configured with:

- **DAG ID:** `email_thread_summaries_to_minio`
- **Schedule:** Daily (`@daily`)
- **Catchup:** Disabled
- **Maximum active runs:** 1
- **Retries:** 1
- **Retry delay:** 5 minutes

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/SUFIYANSK01/lakehouse-pipline.git
```

### 2. Navigate to the project directory

```bash
cd email-thread-summaries-pipeline
```

### 3. Install Python dependencies

If a `requirements.txt` file is available:

```bash
pip install -r requirements.txt
```

Required Python libraries include:

```text
apache-airflow
pandas
PyYAML
pyarrow
minio
```

Use an Airflow-compatible Python environment and install Airflow according to its official installation instructions.

### 4. Configure MinIO

Set the required environment variables and ensure that MinIO is accessible from the Airflow environment.

For local Docker-based deployments, use the MinIO service name as the endpoint if both services share the same Docker network.

### 5. Configure Airflow

Place the DAG file in the Airflow DAGs directory.

Ensure that the source CSV and YAML schema are available at the paths configured in the DAG.

### 6. Run the pipeline

Start your Airflow environment, access the Airflow web interface, locate:

`email_thread_summaries_to_minio`

Enable the DAG and trigger it manually, or allow its daily schedule to execute.

## Key Features

- Automated ETL workflow orchestration.
- YAML-driven schema validation.
- Dynamic data type conversion.
- NULL constraint validation.
- Data cleaning and preprocessing.
- Apache Parquet columnar storage.
- Snappy compression.
- Date-based data partitioning.
- Airflow XCom for inter-task communication.
- Automated MinIO bucket creation.
- S3-compatible object storage integration.
- Logging and exception handling.

## Learning Outcomes

Through this project, I gained practical experience with:

- Building ETL pipelines using Python.
- Orchestrating data workflows using Apache Airflow.
- Implementing schema-driven data validation.
- Working with Pandas and Apache Arrow.
- Understanding Parquet file formats and partitioning.
- Integrating MinIO as an object storage solution.
- Managing task dependencies and data exchange using Airflow XCom.
- Implementing error handling and logging in data pipelines.

## Future Improvements

- Implement incremental data ingestion.
- Add data quality checks and reporting.
- Introduce automated pipeline monitoring and alerting.
- Implement schema evolution.
- Add data deduplication.
- Integrate Apache Spark for large-scale processing.
- Add automated testing using Pytest.

## Author

**Rehan Shaikh**

GitHub: https://github.com/riiiiiiiu

---

*This project demonstrates an end-to-end automated data ingestion workflow using modern data engineering technologies.*
