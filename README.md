# NewsFlow — End-to-End Data Pipeline

Production-grade ETL pipeline with Apache Airflow, Kafka, dbt, and GitHub Actions CI/CD.

## Architecture

**Data Flow:**
1. **Ingest** — Fetch news articles from API
2. **Quality Check** — Validate data integrity
3. **Transform** — dbt models for analytics
4. **Orchestration** — Apache Airflow DAG scheduling
5. **Event Streaming** — Kafka for real-time processing

## Tech Stack

- **Orchestration:** Apache Airflow 3.x
- **Transformation:** dbt with 12 models and 20+ tests
- **Event Streaming:** Apache Kafka (Docker Compose)
- **Database:** PostgreSQL
- **Testing:** pytest with 9 unit tests
- **CI/CD:** GitHub Actions automated testing

## Quick Start

```bash
cd ~/dataops_newsflow
source .venv/bin/activate
export AIRFLOW_HOME=~/dataops_newsflow
airflow standalone
# Visit http://localhost:8080
```

## Testing

```bash
pytest tests/ -v --cov=scripts
```

## Project Structure
dataops_newsflow/
├── dags/ # Airflow DAG definitions
├── scripts/ # Python ETL scripts
├── newsflow_dbt/ # dbt transformation models
├── tests/ # Unit tests
├── docker-compose.yml # Kafka, PostgreSQL, Zookeeper
├── requirements.txt # Python dependencies
└── README.md


## Features

- ✅ Automated testing on every commit
- ✅ Event-driven ETL with Kafka
- ✅ Data quality gates before transformation
- ✅ Containerized infrastructure
- ✅ CI/CD pipeline with GitHub Actions
