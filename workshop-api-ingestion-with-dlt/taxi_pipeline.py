import requests
import dlt

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
PAGE_SIZE = 1000

def fetch_pages():
    page = 1
    while True:
        params = {"page": page, "page_size": PAGE_SIZE}
        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        # Stop when API returns an empty page
        if not data:
            break

        yield from data
        page += 1

@dlt.resource(name="yellow_taxi_trips", write_disposition="replace")
def yellow_taxi_trips():
    yield from fetch_pages()

def main():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
    )
    info = pipeline.run(yellow_taxi_trips())
    print(info)

if __name__ == "__main__":
    main()
