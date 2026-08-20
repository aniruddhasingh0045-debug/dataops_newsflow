from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

articles = [
    {'title': 'AI Advances in 2024', 'description': 'Major breakthroughs', 'url': 'http://example.com/1', 'source': 'Tech News'},
    {'title': 'Data Engineering Trends', 'description': 'Kafka dominates', 'url': 'http://example.com/2', 'source': 'Data Weekly'},
    {'title': 'Cloud Computing Growth', 'description': 'AWS, Azure, GCP compete', 'url': 'http://example.com/3', 'source': 'Cloud Times'},
    {'title': 'PostgreSQL Updates', 'description': 'Performance improvements', 'url': 'http://example.com/4', 'source': 'DB News'},
    {'title': 'Docker Best Practices', 'description': 'Containerization evolves', 'url': 'http://example.com/5', 'source': 'DevOps Daily'},
]

try:
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
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
