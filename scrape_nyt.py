import dlt
import requests
from bs4 import BeautifulSoup
from datetime import datetime

@dlt.resource(write_disposition="replace")
def nyt_politics_headlines():
    url = "https://www.nytimes.com/section/politics"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # NYT section pages usually have headlines in <h3> tags or within 'css-...' classes
    articles = []
    # This selector targets the common article structure on section pages
    for article in soup.find_all('li', class_=lambda x: x and 'css-' in x):
        headline_tag = article.find('h3')
        link_tag = article.find('a')
        summary_tag = article.find('p')
        
        if headline_tag and link_tag:
            title = headline_tag.get_text(strip=True)
            link = "https://www.nytimes.com" + link_tag['href'] if link_tag['href'].startswith('/') else link_tag['href']
            summary = summary_tag.get_text(strip=True) if summary_tag else ""
            
            articles.append({
                "title": title,
                "url": link,
                "summary": summary,
                "scraped_at": datetime.now().isoformat()
            })
    
    # Fallback: if list items didn't work, try all h3 tags
    if not articles:
        for h3 in soup.find_all('h3'):
            parent_a = h3.find_parent('a')
            if parent_a:
                articles.append({
                    "title": h3.get_text(strip=True),
                    "url": "https://www.nytimes.com" + parent_a['href'] if parent_a['href'].startswith('/') else parent_a['href'],
                    "summary": "",
                    "scraped_at": datetime.now().isoformat()
                })

    yield articles

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="nyt_scraper",
        destination="duckdb",
        dataset_name="nyt_data",
    )
    
    load_info = pipeline.run(nyt_politics_headlines())
    print(load_info)
