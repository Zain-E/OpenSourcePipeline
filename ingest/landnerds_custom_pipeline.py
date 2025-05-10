import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import dlt
import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
from constants import BIGQUERY_SERVICE_ACCOUNT_FILE, PROJECT_ID
from tqdm import tqdm
import math


def gbq_to_df(table_name: str, desired_chunk_size: int) -> pl.DataFrame:
    """
    Load data from a BigQuery table in chunks and output as a Polars DataFrame.
    
    Args:
        table_name (str): The (full) name of the BigQuery table to load
        desired_chunk_size (int): The desired size of each chunk to load
    Returns:
        pl.DataFrame: A Polars DataFrame containing the loaded data.
    
    """
    # Method is still far too slow! (around 4 hours for 4.5 million rows)


    credentials = service_account.Credentials.from_service_account_file(BIGQUERY_SERVICE_ACCOUNT_FILE)
    project_id = PROJECT_ID
    client = bigquery.Client(credentials=credentials, project=project_id)

    count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
    total_rows = client.query(count_query).result().to_dataframe()["row_count"][0]
    total_chunks = math.ceil(total_rows / desired_chunk_size)


    all_chunks = []

    for i in tqdm(range(total_chunks), desc=f"Loading chunks from BigQuery table: {table_name}"):
        lower = i * desired_chunk_size
        upper = (i + 1) * desired_chunk_size

        sql = f"""
        WITH numbered AS (
            SELECT *, ROW_NUMBER() OVER () AS rn
            FROM {table_name}
        )
        SELECT * EXCEPT(rn)
        FROM numbered
        WHERE rn > {lower} AND rn <= {upper}
        """

        query = client.query(sql)
        rows = query.result()
        df = pl.from_arrow(rows.to_arrow())

        if df.is_empty():
            tqdm.write(f"No more data at chunk {i+1}. Stopping.")
            break

        all_chunks.append(df)

    if all_chunks:
        final_df = pl.concat(all_chunks, how="vertical")
        return final_df
    else:
        raise ValueError("No data was loaded from BigQuery.")

def gbq_to_file_system(full_table_name: str) -> None:
    """
    Landnerds pipeline to load from a df to s3 storage.
    
    """
    table_name = full_table_name.split(".")[-1]

    data = gbq_to_df(full_table_name, 10000)
    records = data.to_dicts()
    resource = dlt.resource(records, name=table_name)

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="sql_database_landnerds",
        destination="filesystem",
        dataset_name="landnerds"
    )

    # Run the pipeline
    load_info = pipeline.run(resource, loader_file_format="parquet")

    # Pretty print load information
    print(load_info)

if __name__ == '__main__':
    gbq_to_file_system("landnerds.3_data_analytics.all_census_2021")