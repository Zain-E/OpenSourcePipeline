import dlt
from dlt.sources.rest_api import (
    rest_api_source,
    check_connection,
)
from dlt.common.pendulum import pendulum
import requests
from datetime import datetime
import polars as pl


def fetch_pokemon():
    """
    Fetch a list of all Pokémon from the PokeAPI.
    Returns:
        list: A list of dictionaries containing Pokémon names and URLs.
    """
    url = "https://pokeapi.co/api/v2/pokemon?limit=100000&offset=0"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["results"]


def fetch_pokemon_details(url):
    """
    Fetch detailed information about a Pokémon from the given URL.
    Args:
        url (str): The URL to fetch Pokémon details from.
    Returns:
        dict: A dictionary containing detailed information about the Pokémon.
    """
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    moves = [
        move["move"]["name"] for move in data["moves"]
    ]  # Extract move names only and add to rows as list
    return {
        "id": data["id"],
        "order": data["order"],
        "name": data["name"],
        "height": data["height"],
        "weight": data["weight"],
        "species": data["species"],
        "stats": data["stats"],
        "types": data["types"],
        "base_experience": data["base_experience"],
        "moves": moves,
        "abilities": data["abilities"],
        "location_area_encounters": data["location_area_encounters"],
        "sprites": data["sprites"],
        "cries": data["cries"],
        "forms": data["forms"],
        "game_indices": data["game_indices"],
        "date_fetched": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def pokemon_details_df() -> pl.DataFrame:
    """
    Fetches a list of all pokémon names and passes through fetch_pokemon_details to get detailed information for all pokémon.
    Returns:
        pl.DataFrame: A Polars DataFrame containing detailed information about all Pokémon.
    """
    pokemon_list = fetch_pokemon()
    all_pokemon = []
    for pokemon in pokemon_list:
        details = fetch_pokemon_details(pokemon["url"])
        all_pokemon.append(details)
    df = pl.DataFrame(all_pokemon)
    print(f"completed {len(pokemon_list)} records")
    print(all_pokemon[:10])
    return df


def df_to_file_system(df: pl.DataFrame) -> str:
    """
    Pipeline to load from a df to s3 storage.

    Args:
        full_table_name (str): The (full) name of the BigQuery table to load
        df (pl.DataFrame): The Polars DataFrame to load
    Returns:
        statement indicating the table has been saved to the filesystem.

    """
    table_name = "pokemon_details"
    arrow_table = df.to_arrow()
    resource = dlt.resource(arrow_table, name=table_name)

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination="filesystem",
        dataset_name="pokemon_api",
    )

    # Run the pipeline
    load_info = pipeline.run(resource, loader_file_format="parquet")

    # Pretty print load information
    print(f"dlt load data: {load_info}")
    print(f"dataset of shape: {df.shape} uploaded!")


def load_pokemon() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination="filesystem",
        dataset_name="pokemon_api",
    )

    pokemon_source = rest_api_source(
        {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                # If you leave out the paginator, it will be inferred from the API:
                # "paginator": "json_link",
            },
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
                "write_disposition": "replace",
            },
            "resources": ["berry", "location", "item", "move", "version"],
        }
    )

    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            pokemon_source,
            "not_existing_endpoint",
        )
        if not can_connect:
            pass

    check_network_and_authentication()

    load_info = pipeline.run(pokemon_source, loader_file_format="parquet")
    print(load_info)


if __name__ == "__main__":
    df_to_file_system(pokemon_details_df())
    load_pokemon()
