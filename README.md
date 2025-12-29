# Spark + ScyllaDB + Airflow Data Engineering Pipeline

## Overview
The pipeline:

1. Generates synthetic transaction data
2. Writes raw data to **Delta Lake**
3. Transforms transactions into daily aggregates using Spark
4. Loads raw and aggregated data into **ScyllaDB**
5. Orchestrates database setup and Spark jobs using **Airflow DAGs**

---

## Architecture

**Components**:

* **Apache Spark** – distributed data processing
* **ScyllaDB** – NoSQL data store (Cassandra-compatible)
* **Apache Airflow** – workflow orchestration
* **Delta Lake** – storage layer for raw transactional data
* **Docker Compose** – local environment orchestration

## Repository Structure

```
de-spark-scylladb-airflow-master/
├── docker-compose.yml         
├── cmds.txt                   
├── dags/                      
│   ├── db_setup.py             
│   └── spark_pipeline_dag.py   
├── scripts/
│   ├── python/                 
│   │   ├── generate_csv.py
│   │   ├── write_delta.py
│   │   ├── transform_daily_totals.py
│   │   ├── ingest_to_scylla.py
│   │   └── ingest_totals_to_scylla.py
│   └── scylladb/               
│       ├── db_init.cql
│       └── target_init.cql
├── data/
│   └── delta-lake/           
└── logs/                   
```

---

## Prerequisites

Ensure the following are installed locally:

* Docker (>= 20.x)
* Docker Compose (v2 recommended)
* At least **8 GB RAM** available for Docker

Optional (for manual execution/debugging):

* Python 3.8+
* Apache Spark client

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/MandarBorhade/de-spark-scylladb-airflow.git
cd de-spark-scylladb-airflow-master
```

---

### 2. Start the Environment

```bash
docker compose up -d
```

---

### 3. Access Services

| Service    | URL                                            |
| ---------- | ---------------------------------------------- |
| Airflow UI | [http://localhost:8081](http://localhost:8081) |
| Spark UI   | [http://localhost:8080](http://localhost:8080) |
| ScyllaDB   | localhost:9042                                 |

**Airflow credentials**:

```
username: admin
password: admin
```

---

## Database Initialization

ScyllaDB schema is created via CQL scripts:

* `db_init.cql` – raw transactions table
* `target_init.cql` – daily aggregation table

These are automatically executed by the Airflow DAG, but can also be run manually:

```bash
docker exec -it scylla cqlsh -f /opt/scripts/scylladb/db_init.cql
docker exec -it scylla cqlsh -f /opt/scripts/scylladb/target_init.cql
```

---

## Running the Pipeline (Airflow)

1. Open the Airflow UI
2. Enable the DAG: **`transaction_etl_pipeline`**
3. Trigger the DAG manually

### DAG Tasks

1. Initialize ScyllaDB tables
2. Generate CSV transaction data
3. Write raw data to Delta Lake
4. Transform daily totals using Spark
5. Ingest raw transactions into ScyllaDB
6. Ingest aggregated totals into ScyllaDB

Logs for each step are available in the Airflow UI and `/logs` directory.

---
## Data Storage

### Delta Lake

* Location: `data/delta-lake`
* Contains raw transaction data

### ScyllaDB

* **Keyspace**: defined in CQL scripts
* Tables:

  * `customer_transactions`
  * `daily_customer_totals`

---


## Demo video link ##
```
https://drive.google.com/file/d/1SDrl6LTYG7_kCwBnrCNS5nZ6f1D81Hv9/view?usp=sharing
```
