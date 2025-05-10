#!/bin/bash
chmod +x run_ingest.sh

# Activate your virtual environment if needed
source .venv/bin/activate

# Run the ingest pipelines
python ingest/pokemon_api_pipeline.py
python ingest/landnerds_custom_pipeline.py
