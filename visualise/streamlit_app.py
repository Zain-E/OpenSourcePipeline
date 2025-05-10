import streamlit as st
import pandas as pd

# Set your S3 path (can be private or public if permissions are open)
S3_PATH = "s3://0-data-lake/"

# Files to load
files = {
    "berry": f"{S3_PATH}pokemon_api/berry/1746392039.231574.1f6b9da038.parquet",
    "pokemon": f"{S3_PATH}pokemon_api/pokemon/1746392039.231574.34890c731f.parquet",
    "item": f"{S3_PATH}pokemon_api/item/1746392039.231574.0b12eb23a4.parquet",
    "location": f"{S3_PATH}pokemon_api/location/1746392039.231574.f195c9f674.parquet",
    "move": f"{S3_PATH}pokemon_api/move/1746392039.231574.d2b98a8e71.parquet"
}

@st.cache_data
def load_data(s3_path):
    df = pd.read_parquet(s3_path, engine="pyarrow")
    return df

st.title("Load Specific Pokémon Data from S3")

# Dropdown to select a file
selected = st.selectbox("Choose a dataset", list(files.keys()))

# Load and show the file
try:
    df = load_data(files[selected])
    st.success(f"Loaded {selected} dataset")
    st.dataframe(df)
except Exception as e:
    st.error(f"Error loading data: {e}")