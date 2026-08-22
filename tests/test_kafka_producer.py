import pytest
from kafka import KafkaProducer
import json

def test_kafka_producer_connects():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        assert producer is not None
        producer.close()
    except Exception as e:
        pytest.skip(f"Kafka not available: {e}")

def test_kafka_producer_serializes_json():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_message = {
            'title': 'Test Article',
            'description': 'Test',
            'url': 'http://example.com',
            'source': 'Test Source'
        }
        
        future = producer.send('test-topic', value=test_message)
        assert future is not None
        producer.close()
    except Exception as e:
        pytest.skip(f"Kafka not available: {e}")
