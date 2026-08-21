from kafka import KafkaProducer
import json
import pandas as pd

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

print(f"Publishing {len(articles)} articles to Kafka...")

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
    print(f"✅ Published: {message['title']}")

producer.flush()
print(f"Successfully published {len(articles)} articles to Kafka")
