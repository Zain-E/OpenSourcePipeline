from typing import Any, Optional
import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)

def load_pokemon() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination='filesystem',
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
            "resources":[
                {
                 "name": "pokemon",
                 "primary_key": "name",
                 "write_disposition": "replace",
                }, 
                "berry", 
                "location", 
                "item", 
                "move",
                "version"
            ],
        }
    )

    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            pokemon_source,
            "not_existing_endpoint",
        )
        if not can_connect:
            pass  # do something with the error message

    check_network_and_authentication()

    load_info = pipeline.run(pokemon_source, loader_file_format="parquet")
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_pokemon()
