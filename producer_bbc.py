import json
import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
from datetime import datetime
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        pass

def scrape_bbc_and_produce(topic):
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'bbc-producer',
        'socket.timeout.ms': 10000
    })

    countries = ["US", "Germany", "Italy", "Myanmar", "Kazakhstan"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    total_count = 0
    extracted_at = datetime.now().isoformat()

    for country in countries:
        print(f"Scraping BBC for {country}...")
        url = f"https://www.bbc.co.uk/search?q={country}"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # BBC search results are typically in data-testid="card" or similar
            # The structure often changes, so we'll look for common search result patterns
            articles = []
            for card in soup.find_all('div', {'data-testid': 'unison-card'}):
                headline_tag = card.find('span', {'data-testid': 'card-headline'})
                link_tag = card.find('a')
                summary_tag = card.find('p', {'data-testid': 'card-description'})
                
                if headline_tag and link_tag:
                    title = headline_tag.get_text(strip=True)
                    link = link_tag['href']
                    if not link.startswith('http'):
                        link = "https://www.bbc.com" + link
                    summary = summary_tag.get_text(strip=True) if summary_tag else ""
                    
                    articles.append({
                        "title": title,
                        "url": link,
                        "summary": summary
                    })

            # Fallback for different BBC search layouts
            if not articles:
                for item in soup.find_all('li', class_=lambda x: x and 'Promo' in x):
                    h3 = item.find('h3')
                    a = item.find('a')
                    p = item.find('p')
                    if h3 and a:
                        articles.append({
                            "title": h3.get_text(strip=True),
                            "url": a['href'] if a['href'].startswith('http') else "https://www.bbc.com" + a['href'],
                            "summary": p.get_text(strip=True) if p else ""
                        })

            for article in articles[:10]: # Limit to top 10 per country for now
                standardized_msg = {
                    "source": "BBC",
                    "country_target": country,
                    "title": article["title"],
                    "url": article["url"],
                    "summary": article["summary"],
                    "published_at": None, # BBC search results don't always have easy dates
                    "extracted_at": extracted_at
                }
                
                producer.produce(
                    topic, 
                    json.dumps(standardized_msg).encode('utf-8'), 
                    callback=delivery_report
                )
                total_count += 1
                producer.poll(0)
            
            print(f"Sent {len(articles[:10])} articles for {country}")
            time.sleep(1) # Be nice to BBC
            
        except Exception as e:
            print(f"Failed to scrape BBC for {country}: {e}")

    producer.flush()
    print(f"Finished. Total BBC articles sent to Kafka: {total_count}")

if __name__ == "__main__":
    TOPIC = "unified_news_topic"
    print("Starting BBC Scraper Producer...")
    scrape_bbc_and_produce(TOPIC)
