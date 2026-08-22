from airflow.decorators import dag, task
from datetime import datetime
import subprocess
import pandas as pd
import os
from kafka import KafkaProducer, KafkaConsumer
import json
import psycopg2

PROJECT_DIR = '/home/aniruddha/dataops_newsflow'
DBT_PATH = os.path.join(PROJECT_DIR, '.venv/bin/dbt')

@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={'retries': 2},
    tags=['newsflow', 'etl', 'kafka'],
    description='Event-driven ETL: Kafka producer → consumer → quality check → dbt'
)
def newsflow_pipeline():

    @task()
    def produce_to_kafka() -> str:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        articles = [
            {'title': 'AI Advances', 'description': 'ML breakthroughs', 'url': 'http://example.com/1', 'source': 'Tech'},
            {'title': 'Data Engineering', 'description': 'Kafka trends', 'url': 'http://example.com/2', 'source': 'Data'},
            {'title': 'Cloud Growth', 'description': 'AWS/Azure/GCP', 'url': 'http://example.com/3', 'source': 'Cloud'},
            {'title': 'PostgreSQL', 'description': 'Performance', 'url': 'http://example.com/4', 'source': 'DB'},
            {'title': 'Docker', 'description': 'Containers', 'url': 'http://example.com/5', 'source': 'DevOps'},
        ]
        
        for article in articles:
            message = {
                'title': article['title'],
                'description': article['description'],
                'url': article['url'],
                'source': article['source'],
                'published_at': pd.Timestamp.now().isoformat(),
                'fetched_at': pd.Timestamp.now().isoformat()
            }
            producer.send('articles', value=message)
        
        producer.flush()
        return 'Kafka topic populated'

    @task()
    def consume_from_kafka(kafka_result: str) -> str:
        consumer = KafkaConsumer(
            'articles',
            bootstrap_servers='localhost:9092',
            group_id='airflow_consumer_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        conn = psycopg2.connect(
            dbname='newsflow',
            user='newsflow_user',
            password='newsflow_pass',
            host='localhost'
        )
        cursor = conn.cursor()
        
        count = 0
        for message in consumer:
            article = message.value
            try:
                query = """
                INSERT INTO articles (title, description, url, source, published_at, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """
                cursor.execute(query, (
                    article['title'],
                    article['description'],
                    article['url'],
                    article['source'],
                    article['published_at'],
                    article['fetched_at']
                ))
                conn.commit()
                count += 1
                if count >= 5:
                    break
            except Exception as e:
                conn.rollback()
                raise
        
        cursor.close()
        conn.close()
        consumer.close()
        return f'Inserted {count} articles'

    @task()
    def quality_check() -> bool:
        conn = psycopg2.connect(
            dbname='newsflow',
            user='newsflow_user',
            password='newsflow_pass',
            host='localhost'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM articles")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        if count <= 0:
            raise ValueError("No articles found")
        
        return True

    @task()
    def run_dbt(passed: bool):
        if passed:
            subprocess.run(
                [DBT_PATH, 'run', '--project-dir', 'newsflow_dbt/newsflow_dbt'],
                cwd=PROJECT_DIR,
                check=True
            )

    kafka_result = produce_to_kafka()
    consumer_result = consume_from_kafka(kafka_result)
    check_passed = quality_check()
    run_dbt(check_passed)

newsflow_pipeline()
