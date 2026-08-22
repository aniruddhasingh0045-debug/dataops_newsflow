import pandas as pd
import json
from pathlib import Path

def test_articles_file_exists():
    articles_path = Path('data/staging/articles.parquet')
    assert articles_path.exists(), "articles.parquet not found"

def test_articles_has_required_columns():
    articles_path = Path('data/staging/articles.parquet')
    if articles_path.exists():
        df = pd.read_parquet(articles_path)
        required_cols = ['title', 'url', 'source_name', 'publishedat']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

def test_no_null_urls():
    articles_path = Path('data/staging/articles.parquet')
    if articles_path.exists():
        df = pd.read_parquet(articles_path)
        assert df['url'].notna().all(), "Found null URLs in data"

def test_min_article_count():
    articles_path = Path('data/staging/articles.parquet')
    if articles_path.exists():
        df = pd.read_parquet(articles_path)
        assert len(df) >= 5, f"Expected at least 5 articles, got {len(df)}"
