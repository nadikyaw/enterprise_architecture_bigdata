import dlt
import requests

@dlt.resource(write_disposition="replace")
def news_resource(api_key):
    # Fetching top headlines from NewsAPI
    url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    yield response.json().get("articles", [])

if __name__ == "__main__":
    # Create a dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="news_pipeline",
        destination="duckdb",
        dataset_name="news_data",
    )
    
    api_key = "23c7113d28694d3a9f91d04084cf2274"
    
    # Run the pipeline
    load_info = pipeline.run(news_resource(api_key))
    
    print(load_info)
