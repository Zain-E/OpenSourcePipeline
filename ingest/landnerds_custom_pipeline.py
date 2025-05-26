import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import dlt
import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
from constants import BIGQUERY_SERVICE_ACCOUNT_FILE, PROJECT_ID
import gcsfs


def gbq_to_gcs_storage(full_table_name: str, bucket_name: str) -> str:
    """
    Load data from a BigQuery table into GCS (Google Cloud Storage).
    This is because the data will be and faster to process in smaller file chunks.
    Saves data in parquet format.

    Args:
        full_table_name (str): The (full) name of the BigQuery table to save
        bucket_name (str): The name of the GCS bucket to save the data
    Returns:
        a statement indicating the table has been saved to GCS.

    """
    credentials = service_account.Credentials.from_service_account_file(
        BIGQUERY_SERVICE_ACCOUNT_FILE
    )
    project_id = PROJECT_ID
    client = bigquery.Client(credentials=credentials, project=project_id)
    project, dataset_id, table_id = full_table_name.split(".")

    destination_uri = "gs://{}/{}".format(bucket_name, f"{table_id}-*.parquet")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.job.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET
    )
    extract_job = client.extract_table(
        table_ref, destination_uri, location="US", job_config=job_config
    )
    extract_job.result()
    return print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )


def gcs_to_df(full_table_name: str, bucket_name: str) -> pl.DataFrame:
    """
    Load multiple files from a GCS (Google Cloud Storage) bucket into a Polars DataFrame (lazily).

    Args:
        full_table_name (str): The (full) name of the BigQuery table to load
        bucket_name (str): The name of the GCS bucket to load the data from
    Returns:
        pl.DataFrame: A Polars DataFrame containing the loaded data.

    """
    credentials = service_account.Credentials.from_service_account_file(
        BIGQUERY_SERVICE_ACCOUNT_FILE
    )
    project_id = PROJECT_ID
    client = bigquery.Client(credentials=credentials, project=project_id)
    _, _, table_id = full_table_name.split(".")
    fs = gcsfs.GCSFileSystem()
    gcs_file_path = f"gs://{bucket_name}/{table_id}-*.parquet"
    try:
        lf = pl.scan_parquet(gcs_file_path)

        print(
            "\nLazy Polars DataFrame created. Collecting results (this will read data):"
        )
        df_collected = lf.collect()  # Data is read and processed here

        print(df_collected.head())
        print(f"\nCollected DataFrame shape: {df_collected.shape}")

    except Exception as e:
        print(f"Error with lazy loading Polars: {e}")

    return df_collected


def df_to_file_system(full_table_name: str, df: pl.DataFrame) -> str:
    """
    Landnerds pipeline to load from a df to s3 storage.

    Args:
        full_table_name (str): The (full) name of the BigQuery table to load
        df (pl.DataFrame): The Polars DataFrame to load
    Returns:
        statement indicating the table has been saved to the filesystem.

    """
    table_name = full_table_name.split(".")[-1]
    arrow_table = df.to_arrow()
    resource = dlt.resource(arrow_table, name=table_name)

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="sql_database_landnerds",
        destination="filesystem",
        dataset_name="landnerds",
    )

    # Run the pipeline
    load_info = pipeline.run(resource, loader_file_format="parquet")

    # Pretty print load information
    print(f"dlt load data: {load_info}")
    print(f"dataset of shape: {df.shape} uploaded!")


if __name__ == "__main__":
    full_table_name = "landnerds.1_data_lake.all_prop_point_of_interest"
    bucket_name = "landnerds/3_data_analytics/all_prop_point_of_interest"

    gbq_to_gcs_storage(full_table_name, bucket_name)
    df = gcs_to_df(full_table_name, bucket_name)
    df_to_file_system(full_table_name, df)
