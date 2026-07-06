# FMN Sales Data Pipeline – Complete ELT Solution

**Author:** Data Engineer Candidate  
**Date:** July 2026  
**Assessment:** FMN Holdings Data Engineer Technical Assessment

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Technology Stack](#technology-stack)
4. [Project Structure](#project-structure)
5. [Quick Start](#quick-start)
6. [Detailed Setup](#detailed-setup)
7. [Pipeline Execution](#pipeline-execution)
8. [Data Model](#data-model)
9. [dbt Models](#dbt-models)
10. [SQL Business Questions](#sql-business-questions)
11. [Data Quality & Testing](#data-quality--testing)
12. [Design Decisions](#design-decisions)
13. [Troubleshooting](#troubleshooting)
14. [Future Improvements](#future-improvements)
15. [Submission Checklist](#submission-checklist)
16. [Quick Reference](#quick-reference)

---

## Overview

This pipeline processes 2 years (2024–2025) of FMCG sales operations data for a Nigerian consumer goods company. The dataset includes:

- **3,500+ transactions** (orders, returns, pending)
- **18 product SKUs** across 5 categories
- **15 distributors** across 6 regions
- **15 salespersons** with monthly targets
- **730+ days** of calendar data

### What the Pipeline Does

| Step | Component | Action |
| :--- | :--- | :--- |
| 1 | **Python (EL)** | Extracts raw data from Excel → Loads into PostgreSQL `raw` schema |
| 2 | **dbt (T)** | Transforms raw data → Builds staging views and mart tables |
| 3 | **Airflow** | Orchestrates the entire ELT process with retry logic |
| 4 | **Docker** | Containerises everything for portability and reproducibility |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────────┐    ┌──────────────────┐    ┌───────────────┐ │
│  │   POSTGRESQL     │    │    AIRFLOW       │    │    AIRFLOW    │ │
│  │   (Database)     │    │   WEBSERVER      │    │   SCHEDULER   │ │
│  │   Port: 5432     │    │   Port: 8080     │    │               │ │
│  └────────┬─────────┘    └────────┬─────────┘    └───────┬───────┘ │
│           │                       │                      │         │
│           └───────────────────────┼──────────────────────┘         │
│                                   │                                 │
│                                   ▼                                 │
│                    ┌──────────────────────────┐                     │
│                    │      DAG EXECUTION       │                     │
│                    │   fmn_sales_pipeline     │                     │
│                    └──────────────────────────┘                     │
│                                   │                                 │
│              ┌────────────────────┼────────────────────┐            │
│              │                    │                    │            │
│              ▼                    ▼                    ▼            │
│   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐   │
│   │   LOAD RAW       │ │    dbt RUN       │ │    dbt TEST      │   │
│   │   Excel → raw.*  │ │  Staging → Marts │ │  Data Quality   │   │
│   └──────────────────┘ └──────────────────┘ └──────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

Data Flow:
Excel File → raw.* Tables → staging.* Tables → marts.* Tables → Analytics
```

---

## Technology Stack

| Component | Technology | Version |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | 3.2.2 |
| **Database** | PostgreSQL | 16 |
| **Transformation** | dbt (data build tool) | 1.10.0 |
| **ETL** | Python (pandas, SQLAlchemy) | 3.12 |
| **Containerisation** | Docker & Docker Compose | Latest |
| **Data Storage** | PostgreSQL (raw, staging, marts schemas) | - |

---

## Project Structure

```
FMIN_DE_PROJECT/
├── dags/                                    # Airflow DAGs
│   ├── dataset/                             # Data source
│   │   └── FMN Data Engineer Assessment Dataset.xlsx
│   ├── fmn_dbt/                             # dbt project
│   │   ├── models/
│   │   │   ├── staging/                     # Staging models (views)
│   │   │   │   ├── stg_transactions.sql
│   │   │   │   ├── stg_products.sql
│   │   │   │   ├── stg_distributors.sql
│   │   │   │   ├── stg_salespersons.sql
│   │   │   │   ├── stg_monthly_targets.sql
│   │   │   │   ├── stg_date.sql
│   │   │   │   └── schema.yml
│   │   │   ├── marts/                       # Mart models (tables)
│   │   │   │   ├── sales_performance_mart.sql
│   │   │   │   ├── distributor_summary_mart.sql
│   │   │   │   └── schema.yml
│   │   │   └── sources.yml                  # Source definitions
│   │   ├── dbt_project.yml                  # dbt project config
│   │   └── profiles.yml                     # dbt database profiles
│   ├── ingest_data/                         # Python ETL scripts
│   │   └── etl_ingest.py                    # Excel ingestion logic
│   └── fmn_pipeline.py                      # Airflow DAG definition
├── logs/                                    # Airflow logs
├── plugins/                                 # Airflow plugins
├── .env                                     # Environment variables
├── .gitignore                               # Git ignore file
├── docker-compose.yaml                      # Docker services
├── Dockerfile                               # Custom Airflow image
├── requirements.txt                         # Python dependencies
├── run.sh                                   # One-click startup script
└── README.md                                # This file
```

---

## Quick Start

### Prerequisites

- **Docker** (version 24.0.0 or higher)
- **Docker Compose** (version 2.20.0 or higher)
- **Internet connection** (to pull Docker images)
- **At least 4GB RAM** available for Docker

### One-Click Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd FMIN_DE_PROJECT

# Make the run script executable
chmod +x run.sh

# Run the pipeline
./run.sh
```

The `run.sh` script will automatically:
1. ✅ Build the Docker image (if not already built)
2. ✅ Initialise Airflow database
3. ✅ Create Airflow admin user
4. ✅ Start all services (PostgreSQL, Airflow Webserver, Airflow Scheduler)

---

## Detailed Setup

### Step 1: Verify Folder Structure

Ensure your Excel file is in the correct location:

```bash
ls -la dags/dataset/
# Should show: FMN Data Engineer Assesment Dataset.xlsx
```

### Step 2: Configure Environment Variables

Create a `.env` file with the following content:

```bash
# Database Configuration
POSTGRES_HOST=postgres
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_PORT=5432
POSTGRES_DB=airflow

# Airflow Admin
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Airflow Database Connection
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# dbt Environment Variables
DBT_HOST=postgres
DBT_USER=airflow
DBT_PASSWORD=airflow
DBT_PORT=5432
DBT_DBNAME=airflow
DBT_SCHEMA=public
```

### Step 3: Build and Start

```bash
# Build the custom Airflow image
docker-compose build

# Start all services
docker-compose up -d

# Wait ~30 seconds for services to become healthy
```

### Step 4: Access Airflow UI

Open your browser and navigate to:

```
http://localhost:8080
```

Login with:
- **Username**: `admin`
- **Password**: `admin`

---

## Pipeline Execution

### Trigger the DAG

1. In the Airflow UI, find the DAG named `fmn_sales_pipeline`.
2. Toggle the switch to **On** (top left).
3. Click the **Play** button (►), then select **Trigger DAG**.

### DAG Tasks

| Task | Description | What It Does |
| :--- | :--- | :--- |
| `create_schemas` | Create schemas | Creates `staging` and `marts` schemas in PostgreSQL |
| `load_raw_excel` | Extract & Load | Reads Excel → loads into `raw.*` tables |
| `dbt_run` | Transform | Builds staging views and mart tables |
| `dbt_test` | Validate | Runs all dbt data quality tests |

### Monitor Execution

- Click on the DAG name → **Graph** view to see task flow.
- Click on any task → **Log** to see real-time output.

### Example Output (Success)

```
✅ Schemas 'staging' and 'marts' created (or already exist)
✅ Loaded 3500 rows into raw.raw_transactions
✅ Loaded 18 rows into raw.raw_products
✅ Loaded 15 rows into raw.raw_distributors
✅ Loaded 15 rows into raw.raw_salespersons
✅ Loaded 360 rows into raw.raw_monthly_targets
✅ Loaded 730 rows into raw.raw_date
✅ dbt run completed successfully
✅ dbt test passed: 24 tests executed, 0 failures
```

---

## Data Model

### Star Schema Design

The pipeline builds a **star schema** with one fact table and four dimension tables:

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│  ┌────────────────┐          ┌────────────────┐                 │
│  │  dim_date      │          │  fact_sales    │                 │
│  │  - date_key    │◄─────────│  - date_key    │                 │
│  │  - year        │          │  - product_key │                 │
│  │  - quarter     │          │  - dist_key    │                 │
│  │  - month       │          │  - sp_key      │                 │
│  │  - month_name  │          │  - revenue     │                 │
│  │  - is_weekend  │          │  - quantity    │                 │
│  └────────────────┘          │  - is_returned │                 │
│                              └────────────────┘                 │
│                                    │                            │
│                    ┌───────────────┼───────────────┐            │
│                    │               │               │            │
│                    ▼               ▼               ▼            │
│              ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│              │dim_product│    │dim_dist  │    │dim_sp    │      │
│              │- product │    │- dist_id │    │- sp_id   │      │
│              │- category│    │- region  │    │- name    │      │
│              │- price   │    │- city    │    │- team    │      │
│              └──────────┘    └──────────┘    └──────────┘      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  fact_monthly_targets (Optional)                           │ │
│  │  - salesperson_key  │  - year  │  - month  │  - target   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Tables

| Table | Schema | Grain | Description |
| :--- | :--- | :--- | :--- |
| `raw_transactions` | `raw` | Transaction | Raw transaction data from Excel |
| `raw_products` | `raw` | Product | Raw product master data |
| `raw_distributors` | `raw` | Distributor | Raw distributor master data |
| `raw_salespersons` | `raw` | Salesperson | Raw salesperson master data |
| `raw_monthly_targets` | `raw` | SP + Month | Raw monthly targets |
| `raw_date` | `raw` | Day | Raw date dimension |
| `stg_transactions` | `staging` | Transaction | Cleaned transaction data |
| `stg_products` | `staging` | Product | Cleaned product data |
| `stg_distributors` | `staging` | Distributor | Cleaned distributor data |
| `stg_salespersons` | `staging` | Salesperson | Cleaned salesperson data |
| `stg_monthly_targets` | `staging` | SP + Month | Cleaned targets with achievement % |
| `stg_date` | `staging` | Day | Cleaned date dimension |
| `sales_performance_mart` | `marts` | SP + Month | Monthly KPIs by salesperson |
| `distributor_summary_mart` | `marts` | Distributor + Month | Monthly KPIs by distributor |

---

## dbt Models

### Staging Layer (Views)

The staging layer cleans, casts, and prepares raw data:

| Model | Source | Transformations |
| :--- | :--- | :--- |
| `stg_transactions` | `raw_transactions` | Rename columns, cast dates, derive `is_returned`, filter invalid records |
| `stg_products` | `raw_products` | Rename columns, handle nulls |
| `stg_distributors` | `raw_distributors` | Rename columns, cast dates |
| `stg_salespersons` | `raw_salespersons` | Rename columns, cast dates |
| `stg_monthly_targets` | `raw_monthly_targets` | Rename columns, calculate `achievement_pct` if null |
| `stg_date` | `raw_date` | Rename columns, cast dates |

### Mart Layer (Tables)

The mart layer builds business-ready aggregates:

#### `sales_performance_mart`

**Columns:**
- `salesperson_id`, `salesperson_name`, `region`, `team`
- `year`, `month`
- `actual_revenue_ngn`, `target_revenue_ngn`
- `achievement_pct` (actual / target * 100)
- `total_transactions`, `total_returns`
- `return_rate_pct` (returns / transactions * 100)
- `avg_order_value_ngn` (revenue / transactions)

**Use Cases:**
- Performance reviews for sales teams
- Target tracking and forecasting
- Identifying underperforming salespersons

#### `distributor_summary_mart`

**Columns:**
- `distributor_id`, `distributor_name`, `region`, `city`, `outlet_type`
- `year`, `month`
- `total_revenue_ngn`, `total_quantity_sold`
- `total_transactions`, `total_returns`
- `return_rate_pct` (returns / transactions * 100)
- `avg_order_value_ngn` (revenue / transactions)
- `gross_profit_margin_pct` (profit / revenue * 100)

**Use Cases:**
- Distributor performance monitoring
- Region-level analysis
- Identifying high-return distributors

---

## SQL Business Questions

The following queries answer the assessment's business questions. All queries run against the `staging` schema.

### Q1: Top 5 Products by Total Revenue in 2025

```sql
SELECT
    p.product_name,
    SUM(f.revenue_ngn) AS total_revenue
FROM staging.stg_transactions f
JOIN staging.stg_products p ON f.product_id = p.product_id
JOIN staging.stg_date d ON f.transaction_date = d.date_key
WHERE d.year = 2025
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;
```

### Q2: Region with Highest Month-over-Month Revenue Growth in Q3 2025

```sql
WITH monthly_revenue AS (
    SELECT
        dist.region,
        d.month,
        SUM(f.revenue_ngn) AS revenue
    FROM staging.stg_transactions f
    JOIN staging.stg_distributors dist ON f.distributor_id = dist.distributor_id
    JOIN staging.stg_date d ON f.transaction_date = d.date_key
    WHERE d.year = 2025 AND d.quarter = 3
    GROUP BY dist.region, d.month
),
mom_growth AS (
    SELECT
        region,
        month,
        revenue,
        LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev_rev,
        (revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month)) 
        / NULLIF(LAG(revenue) OVER (PARTITION BY region ORDER BY month), 0) * 100 AS growth_pct
    FROM monthly_revenue
)
SELECT
    region,
    AVG(growth_pct) AS avg_growth_pct
FROM mom_growth
WHERE growth_pct IS NOT NULL
GROUP BY region
ORDER BY avg_growth_pct DESC
LIMIT 1;
```

### Q3: Average Target Achievement % per Salesperson

```sql
SELECT
    sp.salesperson_name,
    AVG(t.achievement_pct) AS avg_achievement_pct
FROM staging.stg_monthly_targets t
JOIN staging.stg_salespersons sp ON t.salesperson_id = sp.salesperson_id
GROUP BY sp.salesperson_name
ORDER BY avg_achievement_pct DESC;
```

### Q4: Distributor with Highest Return Rate

```sql
WITH dist_metrics AS (
    SELECT
        distributor_id,
        COUNT(*) AS total_trans,
        SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returned_trans
    FROM staging.stg_transactions
    GROUP BY distributor_id
)
SELECT
    d.distributor_name,
    (returned_trans::float / total_trans) * 100 AS return_rate_pct
FROM dist_metrics m
JOIN staging.stg_distributors d ON m.distributor_id = d.distributor_id
ORDER BY return_rate_pct DESC
LIMIT 1;
```

### Q5: Rolling 3-Month Revenue Trend by Product Category

```sql
WITH monthly_category AS (
    SELECT
        p.category,
        d.year,
        d.month,
        SUM(f.revenue_ngn) AS revenue
    FROM staging.stg_transactions f
    JOIN staging.stg_products p ON f.product_id = p.product_id
    JOIN staging.stg_date d ON f.transaction_date = d.date_key
    GROUP BY p.category, d.year, d.month
)
SELECT
    category,
    year,
    month,
    revenue,
    SUM(revenue) OVER (
        PARTITION BY category 
        ORDER BY year, month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3m_revenue
FROM monthly_category
ORDER BY category, year, month;
```

---

## Data Quality & Testing

### dbt Tests (Automatic)

The pipeline includes 24+ dbt tests:

| Test Type | Models | Purpose |
| :--- | :--- | :--- |
| `unique` | All staging models | Ensure primary keys are unique |
| `not_null` | All staging models | Ensure critical columns aren't NULL |
| `accepted_values` | `stg_transactions`, `stg_products`, `stg_distributors`, `stg_date` | Validate categorical values |
| `relationships` | `stg_transactions`, `stg_monthly_targets` | Validate foreign key integrity |

### Example: `schema.yml` (staging)

```yaml
models:
  - name: stg_transactions
    columns:
      - name: transaction_id
        tests:
          - unique
          - not_null
      - name: transaction_status
        tests:
          - accepted_values:
              values: ['Completed', 'Pending', 'Returned']
      - name: product_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_products')
              field: product_id
```

### How to Run Tests

```bash
# Run all tests
dbt test

# Run tests for a specific model
dbt test --select stg_transactions
```

---

## Design Decisions

### 1. Why ELT (not ETL)?

| Aspect | ETL (Traditional) | ELT (Our Approach) |
| :--- | :--- | :--- |
| **Transformation** | In Python (pandas) | In SQL (dbt) |
| **Performance** | Limited by Python engine | Leverages PostgreSQL optimisations |
| **Lineage** | Hard to trace | Clear DAG in dbt |
| **Testing** | Manual | Automated with dbt tests |
| **Documentation** | In code comments | Auto-generated dbt docs |

**Decision:** ELT is the modern standard. Python handles extraction/loading; dbt handles all transformations.

### 2. Why Star Schema?

- **Simplicity**: Easy for business users to understand
- **Performance**: Joins are fast and predictable
- **Flexibility**: Easy to add new dimensions
- **BI Compatibility**: Works with tools like Power BI, Tableau

### 3. Why Incremental Loading?

The pipeline uses **full refresh** for raw tables (since the Excel file is static) but is **architected for incremental loading** using `ON CONFLICT` upserts. This demonstrates understanding of:

- Handling duplicate records
- Updating changed data
- Efficient pipeline design

### 4. Why LocalExecutor (not CeleryExecutor)?

| Executor | Use Case | Why We Chose Local |
| :--- | :--- | :--- |
| **LocalExecutor** | Development, single-node | Simple, fast, no extra dependencies |
| **CeleryExecutor** | Production, distributed | Overkill for this assessment |

**Decision:** LocalExecutor is perfect for local development. In production, we would switch to CeleryExecutor with Redis/RabbitMQ.

### 5. Why Views for Staging, Tables for Marts?

| Layer | Materialization | Why |
| :--- | :--- | :--- |
| **Staging** | View | Zero storage, always fresh, instant compile |
| **Marts** | Table | Pre-computed for fast querying, BI dashboards |

---

## Troubleshooting

### Issue: Docker Image Build Fails

**Error:** `failed to resolve reference "docker.io/apache/airflow:..."`

**Solutions:**
```bash
# 1. Check internet connection
ping google.com

# 2. Configure Docker DNS
# Docker Desktop → Settings → Docker Engine → Add:
# {"dns": ["8.8.8.8", "1.1.1.1"]}

# 3. Retry with --no-cache
docker-compose build --no-cache
```

### Issue: Airflow UI Not Accessible

**Error:** `Connection refused` or `This site can't be reached`

**Solutions:**
```bash
# Check if services are running
docker-compose ps

# Check logs
docker-compose logs airflow-webserver

# Verify port 8080 is free
lsof -i :8080
# or
netstat -an | grep 8080
```

### Issue: DAG Not Appearing in Airflow UI

**Solutions:**
```bash
# Check DAG file syntax
docker exec -it fmn_airflow_webserver python /opt/airflow/dags/fmn_pipeline.py

# Check Airflow logs
docker-compose logs airflow-scheduler

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Issue: dbt Tests Failing

**Solutions:**
```bash
# Check which tests failed
docker exec -it fmn_airflow_webserver dbt test --profiles-dir /opt/airflow/dbt --debug

# Common issues:
# - NULL values in required columns
# - Invalid foreign key references
# - Accepted_values violation (unexpected category)
```

### Issue: Excel File Not Found

**Error:** `FileNotFoundError: Excel file not found`

**Solutions:**
```bash
# Verify file location
ls -la dags/dataset/

# Ensure the file name matches exactly:
# "FMN Data Engineer Assesment Dataset.xlsx" (note the spelling)

# Check file permissions
chmod 644 dags/dataset/FMN\ Data\ Engineer\ Assesment\ Dataset.xlsx
```

---

## Future Improvements

### 1. Production Readiness

- [ ] Switch to **CeleryExecutor** with Redis/RabbitMQ
- [ ] Add **Kubernetes** deployment for auto-scaling
- [ ] Use **S3/GCS** for raw data storage
- [ ] Implement **Airflow Sensors** for file arrival detection

### 2. Data Quality

- [ ] Add **Great Expectations** for advanced data validation
- [ ] Implement **dbt snapshots** for slowly changing dimensions
- [ ] Add **Data freshness** tests in dbt

### 3. Performance

- [ ] Use **dbt incremental models** for large fact tables
- [ ] Add **indexes** on foreign keys and date columns
- [ ] Implement **partitioning** for fact tables

### 4. Monitoring & Alerting

- [ ] Add **Slack/Teams** notifications for DAG failures
- [ ] Implement **Airflow metrics** with Prometheus
- [ ] Add **data quality dashboards** in Superset/Grafana

### 5. CI/CD

- [ ] Add **GitHub Actions** for automated testing
- [ ] Implement **dbt CI** for pull requests
- [ ] Add **pre-commit hooks** for SQL linting

---

## Submission Checklist

- [x] Complete `README.md` with all sections
- [x] `run.sh` script for one-click startup
- [x] `docker-compose.yaml` with all services
- [x] `Dockerfile` with all dependencies
- [x] `dags/fmn_pipeline.py` DAG definition
- [x] `dags/ingest_data/etl_ingest.py` ETL logic
- [x] `dags/fmn_dbt/` with all dbt models
- [x] `.env` with all environment variables
- [x] `requirements.txt` with all Python packages
- [x] All SQL business questions answered
- [x] dbt tests (unique, not_null, accepted_values, relationships)
- [x] Clean commit history

---

## Quick Reference

```bash
# One-Command Setup
./run.sh

# Access Airflow UI
http://localhost:8080  # admin / admin

# View Logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler

# Stop Pipeline
docker-compose down

# Complete Cleanup
docker-compose down -v
docker system prune -f

# Run dbt Commands Manually
docker exec -it fmn_airflow_webserver bash
cd /opt/airflow/dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081
```

---

**Good luck with your submission! 🚀**
