# Real-Time Omnichannel Messaging Lakehouse (GCP & Medallion Architecture)

[![GCP](https://img.shields.io/badge/Google%20Cloud-GCS%20%7C%20PubSub%20%7C%20BigQuery-4285F4.svg?logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![Terraform](https://img.shields.io/badge/Terraform-1.7+-7B42BC.svg?logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Apache Kafka](https://img.shields.io/badge/Kafka%20%2F%20Redpanda-Streaming-231F20.svg?logo=apache-kafka&logoColor=white)](https://redpanda.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-E25A1C.svg?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00ADD8.svg)](https://delta.io/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Analytics%20Marts-669DF6.svg?logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Docker](https://img.shields.io/badge/Docker-Local%20Sandbox-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)

An enterprise-grade, end-to-end **Real-Time Streaming Lakehouse** architected for high-velocity conversational messaging platforms (e.g., WhatsApp Business API, Instagram Direct, and Webchat). 

This platform demonstrates the architectural modernization and migration from legacy monolithic cloud stacks (Azure / Databricks) to a cloud-native, decoupled, cost-optimized **Modern Lakehouse on Google Cloud Platform (GCP)**—featuring Infrastructure as Code (**Terraform**), distributed real-time streaming (**PySpark + Kafka/PubSub**), transactional storage (**Delta Lake**), and analytical serving (**BigQuery**).

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Ingestion["1. Real-Time Ingestion (Webhooks & Event Streaming)"]
        W1["WhatsApp Business API"] --> P1["Omnichannel Event Producer<br/>(Python / Pydantic)"]
        W2["Instagram / Webchat"] --> P1
        W3["LLM / Generative AI Bots"] --> P1
        P1 -->|JSON Events| B1["GCP Cloud Pub/Sub / Redpanda<br/>(omnichannel.events.v1)"]
        B1 -.->|Dead Letter| DLQ["omnichannel-messaging-dlq"]
    end

    subgraph IaC["2. Infrastructure as Code (Terraform)"]
        TF["Terraform Modules"] -->|Provisions| GCS_B[("GCS Bronze Bucket<br/>(Lifecycle: Nearline 90d)")]
        TF -->|Provisions| GCS_S[("GCS Silver Bucket<br/>(Versioning Enabled)")]
        TF -->|Provisions| GCS_G[("GCS Gold Bucket")]
        TF -->|Provisions| BQ[("BigQuery Datasets<br/>messaging_analytics")]
        TF -->|Provisions| IAM["IAM Least-Privilege Roles"]
    end

    subgraph Processing["3. Medallion Lakehouse Processing (PySpark & Delta Lake)"]
        B1 -->|Structured Streaming| SP1["Bronze Ingestion Job<br/>(Append-Only Raw JSON)"]
        SP1 --> GCS_B
        
        GCS_B -->|Structured Streaming| SP2["Silver Sanitization Job<br/>(Schema Contract + Watermark Deduplication)"]
        SP2 --> GCS_S
        
        GCS_S -->|Aggregations| SP3["Gold Datamart Job<br/>(KPIs, SLAs, LLM Cost Attribution)"]
        SP3 --> GCS_G
    end

    subgraph Serving["4. Serving & Analytics Layer (BigQuery & BI)"]
        GCS_G -->|External / Native Tables| BQ
        BQ --> M1["Bot Performance Mart<br/>(Handoff Rates & Resolution)"]
        BQ --> M2["LLM Cost & Latency Mart<br/>(Tokens, Latency & Cost in USD)"]
        BQ --> M3["Delivery Funnel Mart<br/>(Sent -> Delivered -> Read)"]
    end
```

---

## Engineering Highlights

### 1. Cloud Modernization: Azure/Databricks to GCP
* **Decoupled Storage & Compute:** Replaced vendor-locked compute instances with serverless GCS object storage and ephemeral Dataproc/PySpark clusters, drastically reducing idle cluster overhead.
* **Storage Tiering (FinOps):** Raw Bronze dumps automatically transition to `NEARLINE` storage after 90 days via Terraform lifecycle rules, cutting cloud storage costs by ~50%.
* **Zero-Copy Serving:** Materialized Gold Delta/Parquet tables are surfaced directly into **Google Cloud BigQuery**, allowing BI analysts to query petabyte-scale data without redundant ETL copies.

### 2. Medallion Architecture Pattern
* **Bronze Layer (Raw):** Append-only raw JSON payloads with ingest audit timestamps (`ingested_at`, `ingest_date`). Preserves the immutable ground truth for auditability and replayability.
* **Silver Layer (Cleaned & Conformed):**
  * Applies strict contract validation using Pydantic and Spark StructTypes.
  * Employs **15-minute tumbling watermarks** and deduplication over `event_id` and `timestamp` to resolve duplicate delivery receipts from external providers (e.g., Meta webhooks).
  * Unpacks nested Generative AI telemetry (prompt/completion tokens, model names, latency).
* **Gold Layer (Business Datamarts):**
  * **`mart_bot_performance`:** Handoff-to-human escalation rate, message volume per channel, distinct active sessions.
  * **`mart_llm_cost`:** Real-time token consumption, average inference latency (ms), and cost attribution in USD per bot and model.
  * **`mart_delivery_funnel`:** End-to-end delivery funnel analysis ($Sent \rightarrow Delivered \rightarrow Read$).

### 3. Data Governance & Privacy (LGPD / GDPR)
* User identifiers (phone numbers, contact IDs) are pseudonymized at the ingestion boundary using **SHA-256 salted hashing** (`user_id_hash`), preventing PII from ever reaching downstream analytical layers.

---

## Repository Structure

```plaintext
realtime-messaging-lakehouse/
├── terraform/                       # GCP Infrastructure as Code
│   ├── main.tf                      # GCS Buckets, Pub/Sub topics, BigQuery datasets
│   ├── variables.tf                 # Project parameters and regions
│   ├── iam.tf                       # Service accounts and IAM bindings
│   ├── outputs.tf                   # Exported resource IDs
│   └── terraform.tfvars.example     # Environment variable blueprint
├── streaming/                       # Ingestion & Event Simulation
│   └── producer/
│       ├── schemas.py               # Omnichannel Pydantic data models
│       ├── event_generator.py       # High-throughput synthetic stream producer
│       └── test_schemas.py          # Unit tests for event validation
├── lakehouse/                       # PySpark Medallion Pipelines
│   ├── bronze/
│   │   └── ingest_raw_stream.py     # Kafka/PubSub -> Bronze Delta streaming
│   ├── silver/
│   │   └── transform_silver.py      # Watermarking, deduplication, schema enforcement
│   └── gold/
│       └── aggregate_gold_marts.py  # KPI marts, LLM token metrics, delivery funnels
├── .github/workflows/               # CI/CD DataOps Automation
│   ├── terraform-ci.yml             # Terraform fmt, init, and validate
│   └── spark-tests.yml              # Pytest schema and pipeline logic verification
├── docker-compose.yml               # Local testing sandbox (Redpanda + MinIO)
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment configuration template
└── README.md                        # Architectural documentation
```

---

## Getting Started: Local Sandbox Simulation

You can test and run this entire platform locally without incurring cloud costs using the included Docker Compose sandbox (Redpanda for Kafka streaming, MinIO for S3/GCS storage emulation, and PySpark).

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.10+

### 2. Environment Setup
```bash
# Clone repository
git clone https://github.com/mksouzza/realtime-messaging-lakehouse.git
cd realtime-messaging-lakehouse

# Setup Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
```

### 3. Launch Local Streaming Infrastructure
```bash
docker compose up -d
```
This spins up:
- **Redpanda Broker:** `localhost:9092`
- **Redpanda Console (Topic UI):** [http://localhost:8085](http://localhost:8085)
- **MinIO Storage Server:** [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`)

### 4. Produce Real-Time Omnichannel Events
Run the synthetic event producer to stream messages into Redpanda:
```bash
python streaming/producer/event_generator.py
```

### 5. Execute Medallion Lakehouse Processing
```bash
# Ingest raw stream into Bronze Delta
python lakehouse/bronze/ingest_raw_stream.py

# Transform and deduplicate into Silver Delta
python lakehouse/silver/transform_silver.py

# Compute analytical Gold datamarts
python lakehouse/gold/aggregate_gold_marts.py
```

---

## Cloud Deployment (Google Cloud Platform with Terraform)

To provision the production infrastructure on GCP:

```bash
cd terraform

# Authenticate with Google Cloud
gcloud auth application-default login

# Copy variables template and configure your project ID
cp terraform.tfvars.example terraform.tfvars

# Initialize and deploy
terraform init
terraform plan
terraform apply
```

---

## Data Engineering Portfolio Ecosystem

This project is part of a comprehensive three-pillar Data Engineering portfolio:

1. **[Real-Time Omnichannel Messaging Lakehouse](https://github.com/mksouzza/realtime-messaging-lakehouse)** (Current Project): Streaming, Medallion Lakehouse, PySpark, Delta Lake, and GCP Terraform.
2. **[Commodities Data Warehouse & Analytics Platform](https://github.com/mksouzza/commodities-data-warehouse)**: Batch ELT, dimensional modeling, dbt-core, PostgreSQL, and Streamlit.
3. **[Airflow Data Pipeline Orchestrator](https://github.com/mksouzza/airflow-data-orchestrator)**: Central workflow orchestration, TaskFlow API, failure alerts, SLA monitoring, and Astro CLI.
