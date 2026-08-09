from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import pandas as pd

@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': 60}
)
def newsflow_pipeline():

    @task()
    def ingest() -> str:
        subprocess.run(
            ['python', 'scripts/ingest.py'],
            check=True
        )
        return 'data/staging/articles.parquet'

    @task()
    def quality_check(path: str) -> bool:
        df = pd.read_parquet(path)
        assert df['url'].notna().all(), 'Null URLs found'
        assert len(df) > 10, 'Too few articles'
        return True

    @task()
    def run_dbt(passed: bool):
        if passed:
            subprocess.run(
                ['dbt', 'run', '--project-dir',
                 'newsflow_dbt/newsflow_dbt'],
                check=True
            )

    run_dbt(quality_check(ingest()))

newsflow_pipeline()
