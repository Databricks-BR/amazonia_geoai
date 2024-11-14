# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/header_notebook.png" style="width: 700px">

# COMMAND ----------

# MAGIC %md
# MAGIC * https://queimadas.dgi.inpe.br/queimadas/dados-abertos/#

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 09-nov-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SHAPES
# MAGIC
# MAGIC ### Queimadas
# MAGIC
# MAGIC * https://terrabrasilis.dpi.inpe.br/queimadas/portal/dados-abertos/#da-area-qmd
# MAGIC
# MAGIC * https://dataserver-coids.inpe.br/queimadas/queimadas/area_queimada/AQ1km/shp
# MAGIC
# MAGIC * https://dataserver-coids.inpe.br/queimadas/queimadas/area_queimada/AQ1km/shp/2002_10_01_aq1km_v6.zip
# MAGIC                         <img src="/static/svg/file_download.svg" alt="Arquivo" width="20" class="svg-icon">
# MAGIC                         2002_10_01_aq1km_v6.zip
# MAGIC                     </a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos CSV mensais
# MAGIC
# MAGIC * https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingestão dos Dados diários de Queimadas - INPE
import pandas as pd
from pyspark.sql.functions import *

# Define URL dos dados a serem baixados:

PATH_URL = f"https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/"
CSV_URL = PATH_URL + "focos_mensal_br_202411.csv"


# Leitura direta da URL e criacao do Dataframe Panda:
df_pd = pd.read_csv(CSV_URL)

# Filtrando os dados para o BIOMA = Amazônia
df_amazonia = df_pd.query('bioma == "Amazônia" ')

# Dataframe Spark
df = spark.createDataFrame(df_amazonia)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map Visualization - Plotly
# MAGIC
# MAGIC ##### Parâmetro:  mapbox_style
# MAGIC * 'open-street-map' - Mapa com a divisão territorial com poucas cores
# MAGIC * 'carto-positron' - Mapa com a divisão territorial sem cor
# MAGIC * 'stamen-terrain' - Mapa com a vegetação
# MAGIC
# MAGIC
# MAGIC ###### Referência (outros parâmetros do Plotly Map):
# MAGIC * https://plotly.com/python-api-reference/generated/plotly.express.scatter_mapbox.html?highlight=scatter_mapbox
# MAGIC

# COMMAND ----------

# DBTITLE 1,Visualização dos Incêndios com Plotly - scatter_mapbox
import plotly.express as px

fig = px.scatter_mapbox(df_amazonia, lat="lat", lon="lon", hover_name="municipio", hover_data=["municipio", "frp"], color_discrete_sequence=["red"], opacity=0.5, zoom=4, height=600)   
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

# COMMAND ----------

# DBTITLE 1,Visualização dos Incêndios com Plotly e Mapa de Satélite da "The National Map (USGS)"
import plotly.express as px

fig = px.scatter_mapbox(df_amazonia, lat="lat", lon="lon", hover_name="municipio", hover_data=["municipio", "frp"], color_discrete_sequence=["red"], opacity=0.5, zoom=4, height=500)   

fig.update_layout(
    mapbox_style="white-bg",
    mapbox_layers=[
        {
            "below": 'traces',
            "sourcetype": "raster",
            "sourceattribution": "United States Geological Survey",
            "source": ["https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"]
        }
      ])
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC * https://geopandas.org/en/stable/docs/user_guide.html
# MAGIC * https://geopandas.org/en/stable/docs.html
# MAGIC

# COMMAND ----------

# Install GeoPandas library
%pip install geopandas

# COMMAND ----------

# MAGIC %pip install geopandas --quiet

# COMMAND ----------

import urllib
import os

def download_file(url):
  filename = url.split('/')[-1] 
  response = urllib.request.urlopen(url)
  content = response.read()
  with open(filename, 'wb' ) as f:
      f.write( content )  
  return filename

# COMMAND ----------

import geopandas as gpd

# Define URL dos dados a serem baixados:
SHAPE_URL = 'https://github.com/Databricks-BR/amazonia_geoai/raw/main/maps/Biomas_5000mil.zip'

geoshape = download_file(SHAPE_URL)
assert(os.path.exists(geoshape))

df_shape = gpd.read_file(geoshape)

display(df_shape.head())

# COMMAND ----------

# MAGIC %md
# MAGIC * h3_coverash3string
# MAGIC * https://docs.databricks.com/en/sql/language-manual/functions/h3_coverash3string.html
# MAGIC * https://docs.databricks.com/en/sql/language-manual/functions/h3_polyfillash3.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The Geometry field may contain POINTS, MULTILINES, POLYGONS and so on. <br>
# MAGIC The dataset may contain more than one geometry field, but only a geometry field can be set as active. <br>
# MAGIC This can be done through the **set_geometry()** function.

# COMMAND ----------

df_geometry = df_shape.set_geometry('geometry')

# Test (View) geometry data
df_geometry.plot()

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots(1, 1, figsize=(15, 15))
df_geometry.plot(ax=ax, column='COD_BIOMA', legend=True, cmap='viridis')

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists  amazonia;
# MAGIC
# MAGIC use catalog amazonia;
# MAGIC
# MAGIC create database if not exists geo;

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/etl_shape.png" style="width: 700px">

# COMMAND ----------

import geopandas as gpd
import urllib
import os

def download_file(url):
  filename = url.split('/')[-1] 
  response = urllib.request.urlopen(url)
  content = response.read()
  with open(filename, 'wb' ) as f:
      f.write( content )  
  return filename
  
def convert_shape_to_delta(url_shape_file, delta_table):

    geoshape = download_file(url_shape_file)
    assert(os.path.exists(geoshape))

    gdf_shape = gpd.read_file(geoshape)

    spark_df_geometry = spark.createDataFrame(gpd.GeoDataFrame(gdf_shape).to_wkt())

    spark_df_geometry.write.mode("overwrite").saveAsTable(delta_table)



# COMMAND ----------


URL_SHAPE =  'https://github.com/Databricks-BR/amazonia_geoai/raw/main/maps/GEOFT_TERRA_INDIGENA.zip'
UC_DELTA_TABLE = f"amazonia.geo.bronze_shp_terra_indigena"

convert_shape_to_delta(URL_SHAPE, UC_DELTA_TABLE)


# COMMAND ----------

URL_SHAPE =  'https://terrabrasilis.dpi.inpe.br/download/dataset/legal-amz-aux/vector/conservation_units_legal_amazon.zip'
UC_DELTA_TABLE = f"amazonia.geo.bronze_shp_conservation_units"

convert_shape_to_delta(URL_SHAPE, UC_DELTA_TABLE)



# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 200;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS amazonia.geo.silver_h3_bioma
# MAGIC AS
# MAGIC (
# MAGIC     SELECT explode(h3_polyfillash3(geometry, 10)) as h3_10_id
# MAGIC     FROM amazonia.geo.bronze_shp_bioma
# MAGIC     WHERE COD_BIOMA = 'AMZ'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 200;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS amazonia.geo.silver_h3_terra_indigena
# MAGIC AS
# MAGIC (
# MAGIC     SELECT explode(h3_polyfillash3(geometry, 10)) as h3_10_id
# MAGIC     FROM amazonia.geo.bronze_shp_terra_indigena
# MAGIC     WHERE terrai_cod = 31101
# MAGIC );
