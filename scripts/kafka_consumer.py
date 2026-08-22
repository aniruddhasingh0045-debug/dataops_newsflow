from kafka import KafkaConsumer
import json
import psycopg2

consumer = KafkaConsumer(
    'articles',
    bootstrap_servers='localhost:9092',
    group_id='newsflow_consumer',
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

print("Listening to Kafka topic 'articles'...")

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
        print(f"✅ Inserted: {article['title'][:50]}...")
        count += 1
        if count >= 5:
            break
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

cursor.close()
conn.close()
consumer.close()
print("✅ Consumer complete!")
