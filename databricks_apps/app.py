# ----------------------------------------------------------------
# LIB DECLARATION
# ----------------------------------------------------------------
import streamlit as st
import pandas as pd
import geopandas as gpd
import time
import json
from keplergl import keplergl
import os
from databricks import sql
from databricks.sdk.core import Config
from streamlit_keplergl import keplergl_static
import h3


# ----------------------------------------------------------------
# PAGE LAYOUT - IMAGE HEADER
# ----------------------------------------------------------------

st.set_page_config(layout="wide")

custom_html = """
<div class="banner">
    <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/header_notebook.png" alt="Amazonia GEO AI">
</div>
<style>
    .banner {
        width: 80%;
        height: 120px;
        overflow: hidden;
    }
    .banner img {
        width: 100%;
        object-fit: cover;
    }
</style>
"""

#st.title("Amazonia GeoAI")

# Display the custom HTML
st.components.v1.html(custom_html)

# Sidebar content
st.sidebar.header("Amazonia Geo AI")
st.sidebar.subheader("Menu")
st.sidebar.link_button("Página inicial", "https://luisassuncaoteste-1444828305810485.aws.databricksapps.com/")
st.sidebar.link_button("Repositório GIT", "https://github.com/Databricks-BR/amazonia_geoai")
st.sidebar.link_button("Referências", "https://github.com/Databricks-BR/amazonia_geoai?tab=readme-ov-file#refer%C3%AAncias")
st.sidebar.link_button("Tire suas dúvidas", "https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/01efa2a4a9ec1e5a88ca3a76e18eda63/chats/01efa2a4b12f15e4adf76161b9640120?o=1444828305810485")
st.sidebar.link_button("Contato", "mailto:amazonia.geoai@databricks.com")

# Main content

#st.write("Welcome to my Streamlit app!")
#st.write("This is the main content area.")

# ----------------------------------------------------------------
# BODY PAGE - GRAPH ANALYSIS
# ----------------------------------------------------------------

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData():

    return sqlQuery("select * from amazonia.geo.gold_h3_score limit 5000")

data = getData()

# st.header("Geo Analysis - Scores:)")
# col1, col2 = st.columns([3, 1])
# with col1:
#     st.scatter_chart(data=data, height=400, width=700, y="fare_amount", x="trip_distance")
# with col2:
#     st.subheader("Predict fare")
#     pickup = st.text_input("From (zipcode)", value="10003")
#     dropoff = st.text_input("To (zipcode)", value="11238")
#     d = data[(data['pickup_zip'] == int(pickup)) & (data['dropoff_zip'] == int(dropoff))]
#     st.write(f"# **${d['fare_amount'].mean() if len(d) > 0 else 99:.2f}**")


# ----------------------------------------------------------------
# BODY PAGE - GEO ANALYSIS - KEPLER.GL
# ----------------------------------------------------------------

# load sampleH3Data from csv and setup the metadata
h3_hex_id_df = sqlQuery("select * from amazonia.geo.gold_h3_score limit 5000")
h3_hex_id_df.label = "H3 Hexagons V2"
h3_hex_id_df.id = "h3-hex-id"

st.subheader("Monitoramento das áreas de risco")


config = {}

config = config = {
                    "mapStyle": {
                        "styleType": "satellite",
                        "topLayerGroups": {},
                        "visibleLayerGroups": {
                            "label": True,
                            "road": True,
                            "border": False,
                            "building": True,
                            "water": True,
                            "land": True,
                        },
                        "mapStyles": {}
                    }
                }

# Create a Kepler.gl map and add the data
map_1 = keplergl.KeplerGl(height=600, config=config)

df = h3_hex_id_df
df['h3_index'] = df['h3_10_id'].apply(lambda x: hex(int(x)).upper().replace("0X",""))

map_1.add_data(data=df, name="h3")

# Display the map in Streamlit
keplergl_static(map_1, center_map=True)

# col1, col2 = st.columns(2)

# with col2:
#     st.dataframe(data=data, use_container_width=True, hide_index=True)

st.scatter_chart(data=data, height=400, width=700, y="impact_score", x="critical_score", color="h3_10_id")















# CÓDIGO ANTIGO

# time.sleep(1.5)
# session_data_ids = []
# if map_config:
#     # map_config_json = map_config.config['config']
#     map_config_json = map_config.config.get('config')

#     # check if any datasets were deleted
#     map_data_ids = [layer["config"]["dataId"] for layer in map_config_json.get("visState").get("layers")]
#     # map_data_ids = []
#     session_data_ids = [dataset.id for dataset in st.session_state.datasets]
#     indices_to_remove = [i for i, dataset in enumerate(st.session_state.datasets) if
#                          not dataset.id in map_data_ids]
#     for i in reversed(indices_to_remove):
#         del st.session_state.datasets[i]

#     session_data_ids = [dataset.id for dataset in st.session_state.datasets]
#     # st.markdown(session_data_ids)

# col1, col2, col3 = st.columns([1, 1, 1])
# with col1:
#     san_diego_button_clicked = st.button('Add Bart Stops Geo', disabled=("bart-stops-geo" in session_data_ids))
#     if san_diego_button_clicked:
#         st.session_state.datasets.append(h3_hex_id_df)
#         st.rerun()

# with col2:
#     bart_button_clicked = st.button('Add SF Zip Geo', disabled=("sf-zip-geo" in session_data_ids))
#     if bart_button_clicked:
#         st.session_state.datasets.append(sf_zip_geo_gdf)
#         st.rerun()

# with col3:
#     h3_button_clicked = st.button('Add H3 Hexagons V2', disabled=("h3-hex-id" in session_data_ids))
#     if h3_button_clicked:
#         st.session_state.datasets.append(h3_hex_id_df)
#         st.rerun()

# st.markdown("""
#      The status of the map is displayed at the bottom of the page. You can zoom in/out, pan map, and 
#      watch the map state change. You can also change the color of different layers, delete data, and 
#      watch the map status change. In other words, Streamlit will be notified of any changes in the 
#      map state, and Streamlit can also dynamically add data to the map.
# """)

# if map_config:
#     st.code(json.dumps(map_config_json, indent=4))
