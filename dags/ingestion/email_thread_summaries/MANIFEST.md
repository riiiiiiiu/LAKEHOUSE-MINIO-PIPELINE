# Dataset Manifest: email_thread_summaries

## Overview
This dataset contains summarized email threads. Each record represents a unique email thread along with its generated summary.

---

## Source

- **Type:** CSV
- **Location:** `dags/extraction/email_thread_summaries.csv`
- **Format:** UTF-8 encoded CSV with header row

---

## Target (MinIO)

- **Bucket:** lakehouse
- **Path:** bronze/email_thread_summaries/
- **File Naming Convention:** email_thread_summaries_<ingestion_ts>.csv

---

## Schema Reference

- Defined in: `schema_expected.yaml`
- Table: `public.email_thread_summaries`

---

## Primary Key

- `thread_id`

---

## Load Strategy

- **Mode:** Append
- **Deduplication:** Based on primary key (`thread_id`)
- **Idempotency:** Required (reprocessing should not create duplicates)

---

## Data Quality Expectations

- `thread_id` must be unique and non-null
- `summary` must be non-null
- No duplicate records based on `thread_id`
---

## Metadata Columns (Optional Future Enhancement)

- `ingestion_timestamp`
- `source_file_name`
---

## Notes

- This is a sample dataset used for pipeline scaffolding.
- Replace `email_thread_summaries.csv` with production data source when available.