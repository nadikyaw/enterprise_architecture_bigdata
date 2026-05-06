import json
import requests
from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Silencing success logs to avoid clutter
        pass

def fetch_and_produce_news(api_key, topic, target_count=250):
    # Added metadata.max.age.ms to help with initial connection
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'news-producer',
        'socket.timeout.ms': 10000
    })
    
    count = 0
    # Different keywords to avoid the 100-result limit per query
    keywords = ["politics", "technology", "science", "business", "health", "sports"]
    
    print(f"Fetching news articles from NewsAPI using multiple keywords...")
    
    for kw in keywords:
        if count >= target_count:
            break
            
        print(f"Querying for keyword: {kw}")
        url = f"https://newsapi.org/v2/everything?q={kw}&pageSize=100&page=1&apiKey={api_key}"
        response = requests.get(url)
        data = response.json()
        
        if data.get("status") != "ok":
            print(f"Error for keyword {kw}: {data.get('message')}")
            continue
            
        articles = data.get("articles", [])
        for article in articles:
            if count >= target_count:
                break
            # Add keyword metadata
            article['search_keyword'] = kw
            producer.produce(topic, json.dumps(article).encode('utf-8'), callback=delivery_report)
            count += 1
            
            # Poll to handle delivery reports
            producer.poll(0)
        
        producer.flush()
        print(f"Total articles sent so far: {count}")
        time.sleep(1)

    print(f"Finished. Total articles sent to Kafka: {count}")

if __name__ == "__main__":
    API_KEY = "23c7113d28694d3a9f91d04084cf2274"
    TOPIC = "news_api_topic"
    
    # Wait for Kafka to be fully ready
    print("Waiting for Kafka to initialize...")
    time.sleep(10)
    
    fetch_and_produce_news(API_KEY, TOPIC, 250)
