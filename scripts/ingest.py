import requests, os, json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load secrets from .env file
load_dotenv()

# Fetch from News API
API_KEY = os.getenv('NEWS_API_KEY')
url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={API_KEY}'

r = requests.get(url, timeout=10)
r.raise_for_status()
articles = r.json().get('articles', [])
print(f'Fetched {len(articles)} articles')

# Load into pandas
df = pd.DataFrame(articles)
print(df.shape)
print(df.info())

# 7 cleaning operations
df = df.dropna(subset=['url'])
df = df.fillna({'author': 'Unknown', 'description': 'No description'})
df = df.drop_duplicates(subset=['url'])
df.columns = df.columns.str.lower()
df['publishedat'] = pd.to_datetime(df['publishedat'], utc=True)
df['source_name'] = df['source'].apply(
    lambda x: x.get('name', '') if isinstance(x, dict) else ''
)
df = df.assign(ingested_at=pd.Timestamp.now())

# Select only needed columns
df = df[['title', 'description', 'url', 'author',
         'source_name', 'publishedat', 'ingested_at']]

# Save to Parquet
Path('data/staging').mkdir(parents=True, exist_ok=True)
df.to_parquet('data/staging/articles.parquet', index=False)
print(f'Saved {len(df)} articles to Parquet')

# Verify
df2 = pd.read_parquet('data/staging/articles.parquet')
print(f'Verified: {len(df2)} rows in Parquet file')
