# Self-Healing Data Pipeline 🚀

A PySpark-based configurable **Self-Healing Data Pipeline** that validates incoming records, automatically heals recoverable data issues, and routes records into different target systems based on data quality.

## Problem Statement

In real-world ETL/ELT pipelines, incoming data often contains:

- Missing values
- Invalid formats
- Duplicate records
- Future timestamps
- Schema mismatches
- Statistical anomalies
- Null-heavy columns

Traditional pipelines fail or drop records.

This project introduces a **self-healing mechanism** where data is automatically corrected whenever possible.

---

# Architecture

Incoming records flow through:

```text
                Source System
                      |
                      v
              +----------------+
              |   Data Reader  |
              +----------------+
                      |
                      v
            +--------------------+
            | Validation Rules   |
            | + Healing Logic    |
            +--------------------+
                      |
      -------------------------------------
      |                  |                |
      v                  v                v

 Correct Records    Healed Records   Unrecoverable Records
(valid)             (auto-fixed)      (dead-letter)

      |                  |                |
      v                  v                v

 Target Table 1    Target Table 2   Target Table 3
```

---

# Record Routing Logic

Pipeline categorizes records into 3 outputs:

### 1. Correct Records

Records that pass all validations.

Example:

```json
{
 "order_id":101,
 "amount":500,
 "email":"abc@gmail.com"
}
```

Stored in:

```text
target = correct
```

---

### 2. Healed Records

Records that failed validation but can be corrected automatically.

Example:

Before:

```json
{
 "amount":-100
}
```

After healing:

```json
{
 "amount":0.01
}
```

Stored in:

```text
target = healed
```

---

### 3. Dead Records (Dead Letter Queue)

Records that cannot be repaired.

Example:

Missing primary keys:

```json
{
 "order_id":null
}
```

Stored in:

```text
target = dead
```

---

# Project Structure

```text
project/

│
├── bin/
│      wrapper.py
│
├── config/
│      rules.json
│      source.json
│
├── dataio/
│      __init__.py
│      read_data.py
│      write_data.py
│
├── transform/
│      code_master.py
│      heal_data.py
│
└── README.md
```

---

# Components

## 1. wrapper.py (Orchestrator)

Main workflow controller.

Responsible for:

- Loading configs
- Creating Spark Session
- Reading source data
- Running transformations
- Writing outputs
- Logging
- Error handling

Pipeline flow:

```text
Load Config
      ↓

Create Spark Session
      ↓

Read Source
      ↓

Apply Rules + Healing
      ↓

Split Dataframes:
correct_df
healed_df
dead_df
      ↓

Write Targets
```

---

## 2. DataReader

Supports multiple sources:

Current:

- PostgreSQL
- MySQL
- Oracle
- SQL Server
- Warehouse
- Object Storage

Uses Spark JDBC readers.

Example:

```python
reader = DataReader(config,spark)
df = reader.data_read()
```

---

## 3. DataWriter

Writes dataframe into targets.

Expected outputs:

```python
writer.data_write(correct_df,"correct")

writer.data_write(healed_df,"healed")

writer.data_write(dead_df,"dead")
```

---

## 4. Transformation Layer

Files:

```text
transform/
    code_master.py
    heal_data.py
```

Responsibilities:

Validation rules

Healing strategies

Record classification

Output splitting

---

# Config Driven Pipeline

Pipeline behaviour is controlled through JSON.

No code change required.

Change:

```json
rules.json
source.json
```

Pipeline behavior changes automatically.

---

# Validation Rules Supported

Current examples:

### Not Null Validation

```json
"type":"not_null"
```

---

### Duplicate Detection

```json
"strategy":"deduplicate"
```

---

### Range Validation

```json
"type":"range"
```

---

### Regex Validation

Email validation:

```json
"type":"regex"
```

---

### Date Validation

Future timestamp detection.

---

### Schema Validation

Missing columns detection.

---

### Statistical Anomaly Detection

Examples:

- Row count spikes
- Volume anomalies

---

# Healing Strategies

Supported healing concepts:

| Validation Failure | Healing Action |
|-------------------|----------------|
| Duplicate rows | Deduplicate |
| Negative amount | Clamp values |
| Invalid email | Nullify |
| Future timestamp | Replace timestamp |
| Missing fields | Fill defaults |
| High null rate | Fill nulls |
| Volume anomaly | Pause + alert |

---

# Example Config Files

## source.json

Contains:

```text
Source DB
Targets
Connection configs
Retry logic
Extraction mode
```

Supports:

- PostgreSQL
- BigQuery
- Local storage

---

## rules.json

Contains:

```text
Validation rules
Healing logic
Thresholds
Strategies
```

---

# Logging

Uses Python logging:

```python
logging.basicConfig(
 level=logging.INFO,
 format="%(asctime)s - %(levelname)s - %(message)s"
)
```

Tracks:

- Reads
- Writes
- Failures
- Healing
- Pipeline execution

---

# Tech Stack

Built using:

- Python
- PySpark
- Spark SQL
- PostgreSQL
- JSON Configurations
- Logging

Future additions:

- Docker
- Kubernetes
- Kafka
- Airflow
- AWS
- GCP
- Databricks

---

# Future Improvements

Planned features:

### Monitoring

- Prometheus
- Grafana

### Alerts

- Slack
- Email
- Webhook

### Orchestration

- Airflow
- Jenkins

### Cloud

- AWS Glue
- EKS
- EMR

### Containers

Dockerize pipeline

Deploy using Kubernetes

---

# How to Run

Install dependencies:

```bash
pip install pyspark
pip install psycopg2
```

Run:

```bash
python bin/wrapper.py
```

---

# Example Execution Flow

Input:

```text
1000 records
```

Output:

```text
850 → Correct

120 → Healed

30 → Dead
```

---

# Goal of This Project

Build an intelligent pipeline that:

✔ Detects bad data

✔ Repairs recoverable data

✔ Separates unrecoverable data

✔ Prevents pipeline failures

✔ Improves data quality automatically

---

# Author

Vedant Chaudhari

Computer Engineering Student

Interested in:

- Data Engineering
- Distributed Systems
- PySpark
- Kubernetes
- Cloud
- ETL Pipelines

---

# License

MIT License