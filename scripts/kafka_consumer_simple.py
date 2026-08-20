from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'articles',
    bootstrap_servers='localhost:9092',
    group_id='newsflow_consumer',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to Kafka topic 'articles'...")

for i, message in enumerate(consumer):
    article = message.value
    print(f"{i+1}. {article['title']}")
    if i >= 4:
        break

print("✅ Consumer test complete!")
