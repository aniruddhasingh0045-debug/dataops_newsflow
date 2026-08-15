from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import pandas as pd
import os

PROJECT_DIR = '/home/aniruddha/dataops_newsflow'
DBT_PATH = os.path.join(PROJECT_DIR, '.venv/bin/dbt')

@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 2},
    tags=['newsflow', 'etl'],
    description='Daily ingest → quality check → dbt transform pipeline'
)
def newsflow_pipeline():

    @task()
    def ingest() -> str:
        subprocess.run(
            ['python', 'scripts/ingest.py'],
            cwd=PROJECT_DIR,
            check=True
        )
        return 'data/staging/articles.parquet'

    @task()
    def quality_check(path: str) -> bool:
        df = pd.read_parquet(path)
        if not df['url'].notna().all():
            raise ValueError("Null URLs found in ingested data")
        if len(df) <= 10:
            raise ValueError(f"Too few articles: {len(df)}")
        return True

    @task()
    def run_dbt(passed: bool):
        if passed:
            subprocess.run(
                [DBT_PATH, 'run', '--project-dir', 'newsflow_dbt'],
                cwd=PROJECT_DIR,
                check=True
            )

    run_dbt(quality_check(ingest()))

newsflow_pipeline()
