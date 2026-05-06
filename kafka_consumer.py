import dlt
import json
from confluent_kafka import Consumer, KafkaException
import time

def consume_and_load(topic, dataset_name):
    # Configure Kafka Consumer
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'news_loaders',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    print(f"Starting consumer for topic: {topic}...")
    
    # Define a simple generator to yield messages for dlt
    def message_generator():
        start_time = time.time()
        timeout = 10  # Stop if no messages for 10 seconds
        received_any = False
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    if received_any and (time.time() - start_time > timeout):
                        print("Timeout reached, closing generator.")
                        break
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Success
                received_any = True
                start_time = time.time() # Reset timeout
                data = json.loads(msg.value().decode('utf-8'))
                yield data
                
        finally:
            consumer.close()

    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="kafka_to_duckdb",
        destination="duckdb",
        dataset_name=dataset_name,
    )
    
    # Run pipeline using the generator
    load_info = pipeline.run(message_generator(), table_name="news_articles")
    print(load_info)

if __name__ == "__main__":
    consume_and_load("unified_news_topic", "unified_news_data")
