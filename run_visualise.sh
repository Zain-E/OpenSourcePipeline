#!/bin/bash
chmod +x run_visualise.sh

# Activate your virtual environment if needed
source .venv/bin/activate

# Run the Streamlit app
python black .
streamlit run visualise/streamlit_app.py
