import streamlit as st
import pandas as pd
import math
from PIL import Image
from pathlib import Path

@st.cache_data
def load_data(s3_path):
    df = pd.read_parquet(s3_path, engine="pyarrow")
    return df

s3_path = f"s3://0-data-lake/yugioh_api/yugioh_cards/1748537410.767642.428df4f96a.parquet"

# Load data
cards_df = load_data(s3_path)

ATTRIBUTE_ICON_URLS = {
    "DARK": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/dark.png",
    "DIVINE": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/divine.png",
    "EARTH": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/earth.png",
    "FIRE": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/fire.png",
    "LIGHT": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/light.png",
    "WATER": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/water.png",
    "WIND": "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/yugioh_assets/wind.png",
}

# If 'data' column contains dictionaries (as strings), parse them
if isinstance(cards_df.iloc[0], str):
    import json
    cards = cards_df["data"].apply(json.loads).tolist()
else:
    cards = cards_df.to_dict(orient="records")

# Map card type into 'Monster', 'Spell', or 'Trap'
def categorize_card_type(card):
    t = card.get("type", "").lower()
    if "monster" in t:
        return "Monster"
    elif "spell" in t:
        return "Spell"
    elif "trap" in t:
        return "Trap"
    else:
        return "Other"

# Add category field to each card
for card in cards:
    card["category"] = categorize_card_type(card)

logo_url = "/Users/zaineisa/Documents/VSCode/OpenSourcePipeline/assets/Yu-Gi-Oh-Logo.png"
st.image(logo_url, width=200)

# Let the user filter by high-level type
type_options = {"Monster", "Spell", "Trap"}

col1, col2, col3 = st.columns(3)

card_type = st.sidebar.radio(
    "",
    options=["Monster", "Spell", "Trap"],
    horizontal=True
)

# Filter cards based on selected category
filtered_cards = [card for card in cards if card["category"] == card_type]

# Extract card names from filtered cards
card_names = [card["name"] for card in filtered_cards]

# Multi-select interface for filtered cards
selected_names = st.sidebar.multiselect(
    "Select Yu-Gi-Oh! card",
    card_names,
    max_selections=1,
)

selected_cards = [card for card in filtered_cards if card["name"] in selected_names]

# UI
for card in selected_cards:
    col1, col2 = st.columns([1, 2])
    with col1:
        st.image(card["card_images"][0]["image_url"], width=150)
        st.image(card["card_images"][0]["image_url_cropped"], width=150)
    with col2:
        st.subheader(card["name"])
        st.markdown(f"_[{card.get('type')} / {card.get('race')}]_")
        st.markdown(f"_[{card.get('archetype', 'N/A')}]_")

        attribute = card.get('attribute')
        icon_path = ATTRIBUTE_ICON_URLS.get(attribute)
        if attribute and icon_path and Path(icon_path).exists():
            col_icon, col_text = st.columns([1, 5])
            with col_icon:
                st.image(icon_path, width=30)
            with col_text:
                st.markdown(f"**{attribute}**")
        else:
            st.markdown(f"**Attribute:** {attribute if attribute else 'None'}")

        level = card.get('level')
        if level is None or (isinstance(level, float) and math.isnan(level)):
            st.markdown("**Level:** N/A")
        else:
            try:
                level_int = int(card.get('level'))
                circles = " 🟠 " * level_int
                st.markdown(f"**Level:** {circles}")
            except Exception:
                st.markdown("**Level:** N/A")

        atk = card.get('atk')
        def_ = card.get('def')
        atk_display = int(atk) if pd.notna(atk) else 'None'
        def_display = int(def_) if pd.notna(def_) else 'None'
        st.markdown(f"**ATK/DEF:** **{atk_display}** / **{def_display}**")
        st.markdown(f"_{card.get('desc')}_")


# Archetype table
if selected_cards:
    selected_card = selected_cards[0]
    selected_archetype = selected_card.get("archetype", None)

    if selected_archetype:
        filtered_by_archetype = [card for card in cards if card.get("archetype") == selected_archetype]

        editor_rows = []
        for card in filtered_by_archetype:
            try:
                editor_rows.append({
                    "image": card['card_images'][0]['image_url'],
                    "name": card['name'],
                    "atk": card.get('atk', 'N/A'),
                    "def": card.get('def', 'N/A'),
                    "price ($)": card['card_prices'][0].get('amazon_price', 'N/A'),
                    "type": card.get('type', 'N/A'),
                    "card_type": categorize_card_type(card),
                    "archetype": card.get('archetype', 'N/A'),
                })
            except Exception as e:
                st.warning(f"Skipping card due to error: {e}")

        archetype_df = pd.DataFrame(editor_rows)

        st.data_editor(
            archetype_df,
            column_config={
                "image": st.column_config.ImageColumn("Card Image", width="small", help="Card description shown on hover"),
                "attribute_icon": st.column_config.ImageColumn("Attribute", width="small"),
            },
            hide_index=True,
            use_container_width=False
        )
    else:
        st.info("This card has no archetype.")
