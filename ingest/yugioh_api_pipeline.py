import dlt
from dlt.sources.rest_api import (
    rest_api_source,
    check_connection,
)
from dlt.common.pendulum import pendulum
import requests
import polars as pl


def fetch_all_cards():
    """
    Fetch a list of all Yu-Gi-Oh card information from the API.
    Returns:
        list: A list of dictionaries containing all card information and URLs.
    """
    url = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    df = pl.DataFrame(data)
    df = df.with_columns([
        pl.lit(pendulum.now().strftime("%Y-%m-%d %H:%M:%S")).alias("date_fetched")
    ])
    return df

df = fetch_all_cards()
print(df.head())
print(df.shape)


def df_to_file_system(df: pl.DataFrame) -> str:
    """
    Pipeline to load from a df to s3 storage.

    Args:
        full_table_name (str): The (full) name of the BigQuery table to load
        df (pl.DataFrame): The Polars DataFrame to load
    Returns:
        statement indicating the table has been saved to the filesystem.

    """
    table_name = "yugioh_cards"
    arrow_table = df.to_arrow()
    resource = dlt.resource(arrow_table, name=table_name)

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_yugioh",
        destination="filesystem",
        dataset_name="yugioh_api",
    )

    # Run the pipeline
    load_info = pipeline.run(resource, loader_file_format="parquet")

    # Pretty print load information
    print(f"dlt load data: {load_info}")
    print(f"dataset of shape: {df.shape} uploaded!")

if __name__ == "__main__":
    df_to_file_system(fetch_all_cards())