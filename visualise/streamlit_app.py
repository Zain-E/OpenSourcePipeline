import streamlit as st
import pandas as pd
import plotly.express as px

# Set your S3 path (can be private or public if permissions are open)
S3_PATH = "s3://0-data-lake/"

# Files to load
files = {
    "berry": f"{S3_PATH}pokemon_api/berry/",
    "pokemon": f"{S3_PATH}pokemon_api/pokemon_details/",
    "item": f"{S3_PATH}pokemon_api/item/",
    "location": f"{S3_PATH}pokemon_api/location/",
    "move": f"{S3_PATH}pokemon_api/move/",
    "version": f"{S3_PATH}pokemon_api/version/"
}

@st.cache_data
def load_data(s3_path):
    df = pd.read_parquet(s3_path, engine="pyarrow")
    return df

st.title("Pokémon Data")

# Dropdown to select a file
selected = st.selectbox("Choose a dataset", list(files.keys()))

try:
    df = load_data(files[selected])
    st.success(f"Loaded {selected} dataset")
    # st.dataframe(df)
except Exception as e:
    st.error(f"Error loading data: {e}")

if files["pokemon"]:

    pokemon_names = df["name"].unique().tolist()
    selected_pokemon = st.selectbox("Select Name", pokemon_names,index=None)

    # Step 3: Display Pokémon Details
    if selected_pokemon is None:
        data = st.dataframe(df)
    else:
        filtered = df[df["name"] == selected_pokemon]
        data = st.dataframe(filtered)
        if selected == "pokemon":
            # Create Columns for Side-by-Side Layout
            col1, col2, col3 = st.columns(3)

            # Column 1: Regular Pokémon Sprite
            with col1:
                st.image(data["sprites"]["other"]["dream_world"]["front_default"], width=100, caption="Artwork")

            # Column 2: Dream World Artwork
            with col2:
                gif_sprite = data["sprites"]["versions"]["generation-v"]["black-white"]["animated"]["front_default"]
                if gif_sprite:
                    st.image(gif_sprite, width=100, caption="Animated Sprite")
                else:
                    st.write("No GIF available")

            # Column 3: Animated (GIF) Sprite
            with col3:
                st.image(data["sprites"]["front_default"], width=100, caption="Default Sprite")

            # Pokémon Cry (Sound)
            cry_url = f"https://play.pokemonshowdown.com/audio/cries/{selected_pokemon.lower()}.mp3"  # Pokémon cry URL
            try:
                st.audio(cry_url, format="audio/mp3")
            except:
                st.write("No cry available for this Pokémon.")

            else:
                st.write("No version information available for this Pokémon.")
