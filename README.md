# Accord API Data Ingestion Service

![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-15-blue.svg)
![Docker](https://img.shields.io/badge/docker-compose-blue.svg)

An enterprise-grade, highly resilient data ingestion pipeline designed to fetch, validate, transform, and load financial market data from the **Accord Web Services API** into a local PostgreSQL database.

This service acts as the central ingestion nervous system for the FinGlobal platform. It relies on a continuous background scheduler to automate different feed batches at precise times throughout the day:
* **Hourly**: Periodic financial results and performance updates.
* **Intraday**: Structural updates (Company Master, Boards) running several times a day.
* **End-of-Day (EOD)**: Large bulk market pricing and end-of-day metrics.

---

## 🏗 System Architecture

The application is structured into isolated functional micro-services orchestrated entirely via **Docker Compose**:

1. **`db`**: A PostgreSQL 15 persistent database housing the financial models, temporary staging structures, ingestion metrics, and raw JSON payloads.
2. **`scheduler`**: A continuous background daemon leveraging `APScheduler` to trigger API fetch cycles strictly tied to the Accord API publication timetable.
3. **`api_manual`**: An ephemeral container used to manually force ingestion loops for testing or ad-hoc data synchronization.
4. **`backfill`**: An ephemeral container executing a strict 7-day chronological historical recovery loop.

---

## 🌊 Data Flow & Pipeline Stages

1. **Fetch**: Requests are fired via `accord_client.py` requesting incremental data since the specified date.
2. **Validate Payload**: `validation_service.py` intercepts the JSON response, verifying Primary Key integrity, schema boundaries, and auditing structural flags (A/O/D). Massive unexpected deletion events are actively blocked.
3. **Normalize**: `normalizer.py` converts custom Accord naming conventions into standardized Postgres-compliant snake_case attributes.
4. **Stage & Diff**: `staging_loader.py` writes the normalized payload into an ephemeral `staging_table`.
5. **Foreign Key Protection**: `merge_service.py` ensures referential integrity. Sub-records (e.g., Financial statements) missing their parent `Company_master` entity are caught and safely parked in a persistent queue (`rejected_ingestion_rows`).
6. **Merge (Upsert)**: A high-performance SQL upsert merges the clean staging data into the live persistent database target.
7. **Retry Mechanics**: The `retry_service.py` ambulance routing automatically picks up parked Foreign Key rejections and periodically re-attempts ingestion as parent records populate asynchronously.
8. **Logging & Summarization**: `ingestion_log.py` updates the `ingestion_runs` metrics and calculates `daily_ingestion_summary` dashboards for observability.

---

## 🛡️ Production Hardening Features

This service is fully fortified against network failures, data corruption, and redundant operations:

* **Strict Idempotency Guard**: Repeated executions within the same day are aggressively skipped unless `force=True` is provided, saving network bandwidth and compute overhead.
* **Network Resilience**: Circuit breakers and exponential backoff loops (2s, 5s, 10s) protect against HTTP 429 Rate Limits and 5xx API outages.
* **Cascading Retries**: Rejected structural rows do not fail the pipeline; they are vaulted and re-attempted automatically up to 5 times before firing a manual intervention alert.
* **Daily Aggregation**: Real-time aggregation tables track `feeds_success`, `rows_upserted`, `rows_rejected`, and `total_duration_seconds`.
* **Zero Hardcoding**: All critical tolerances, database strings, and retry configurations are isolated safely inside the `.env` context.

---

## ⚙️ Environment Configuration

You must create a `.env` file in the root directory prior to executing Docker commands.

```env
# =============================================================================
# DATABASE CONFIG
# =============================================================================
POSTGRES_USER=admin
POSTGRES_PASSWORD=password123
POSTGRES_DB=financial_db
DATABASE_URL=postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}

# =============================================================================
# ACCORD API CONFIG
# =============================================================================
ACCORD_API_TOKEN=YOUR_REAL_ACCORD_TOKEN_HERE

# =============================================================================
# API INGESTION HARDENING
# =============================================================================
ENABLE_IDEMPOTENCY=true
ENABLE_REJECTED_RETRY=true
MAX_REJECTED_ROW_RETRY=5
ALLOW_MASS_DELETE=false
MAX_CONSECUTIVE_FAILURES=3

# =============================================================================
# API CLIENT RETRY CONFIG
# =============================================================================
API_MAX_RETRIES=3
API_RETRY_BACKOFF_1=2
API_RETRY_BACKOFF_2=5
API_RETRY_BACKOFF_3=10
API_TIMEOUT_SECONDS=60
```
*(Reference the full `.env` template in the repository for scheduler and alerting properties.)*

---

## 🚀 Deployment & Operations

### 1. Booting the Application
Spin up the persistent database and the automated scheduler background process:
```bash
docker compose up -d db scheduler
```

### 2. Manual Interactions
Need to force a daily sync or run a historical backfill? Execute the isolated containers:
```bash
# Run a one-off daily synchronization:
docker compose run --rm api_manual

# Run the chronological 7-day backfill sequence:
docker compose run --rm backfill
```

### 3. Quick Operations Cheat Sheet

**SSH Access to Host:**
```bash
ssh -i ~/.ssh/finglobal-api-data-ingestion_key.pem azureuser@4.188.80.28
cd ~/finglobal_api_ingestion_service
sudo apt update
```

**Checking Active Volumes:**
```bash
sudo docker compose exec db psql -U admin -d financial_db -c "SELECT COUNT(*) FROM company_master;"
sudo docker compose exec db psql -U admin -d financial_db -c "SELECT COUNT(*) FROM board;"
```

**Validation & Monitoring Queries:**
```sql
-- View all rejected rows still pending retry
SELECT feed_name, retry_count, reason FROM rejected_ingestion_rows WHERE resolved = FALSE;

-- View your daily aggregation stats
SELECT * FROM daily_ingestion_summary ORDER BY summary_date DESC;

-- Identify feeds that failed or triggered alerts today
SELECT * FROM ingestion_runs WHERE status != 'SUCCESS' AND requested_date = CURRENT_DATE;
```

---

## 📂 Project Structure

```text
finglobal_api_ingestion_service/
├── .env                        # Root configuration matrix
├── docker-compose.yml          # Container orchestration topology
├── app/
│   ├── accord_client.py        # HTTP networking & backoff mechanics
│   ├── alert_service.py        # External notification gateway
│   ├── api_main.py             # Feed iteration & core lifecycle loop
│   ├── config.py               # Environment variable parsing engine
│   ├── db.py                   # SQLAlchemy connection pools
│   ├── ingestion_log.py        # Metrics recording & summarization
│   ├── merge_service.py        # FK checks & UPSERT generation
│   ├── normalizer.py           # Column name standardization
│   ├── retry_service.py        # Rejected row ambulance routing
│   ├── scheduler.py            # APScheduler cron framework
│   ├── staging_loader.py       # Ephemeral table management
│   └── validation_service.py   # Payload verification & flag security
├── sql/
│   └── ingestion_tables.sql    # DDL for tracking & metric tables
```