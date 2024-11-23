# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/header_notebook.png" style="width: 700px">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version Code Control
# MAGIC
# MAGIC | versão | data | autor | e-mail | alterações |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1.0 | 14-nov-2024 | Luis Assunção | luis.assuncao@databricks.com | Primeira versão |

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/logo_prodes.png">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DESMATAMENTO 
# MAGIC #### Supressão da vegetação nativa para a Amazônia - BIOMA
# MAGIC
# MAGIC O PRODES, ou Projeto de Monitoramento do Desmatamento na Amazônia Legal por Satélite, é um projeto do Instituto Nacional de Pesquisas Espaciais (INPE) que monitora o desmatamento por corte raso na Amazônia Legal e produz desde 1988 as taxas anuais de desmatamento na região. Os valores são estimados a partir dos incrementos de desmatamento identificados em cada imagem de satélite que cobre a área estudada. O projeto é financiado pelo Ministério da Ciência, Tecnologia e Inovações e conta com a colaboração do Ministério do Meio Ambiente e do IBAMA.
# MAGIC
# MAGIC ##### Referências:
# MAGIC * https://pt.wikipedia.org/wiki/PRODES
# MAGIC * http://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos Shapefile
# MAGIC
# MAGIC * https://terrabrasilis.dpi.inpe.br/download/dataset/amz-prodes/vector/prodes_amazonia_nb.gpkg.zip
# MAGIC
# MAGIC * https://terrabrasilis.dpi.inpe.br/download/dataset/amz-prodes/vector/yearly_deforestation_smaller_than_625ha_biome.zip
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/amazonia_geoai/main/images/etl_shape.png" style="width: 700px">

# COMMAND ----------

# MAGIC %pip install geopandas --quiet

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


URL_SHAPE =  'https://terrabrasilis.dpi.inpe.br/download/dataset/amz-prodes/vector/yearly_deforestation_smaller_than_625ha_biome.zip'
UC_DELTA_TABLE = f"amazonia.geo.bronze_shp_desmatamento"

convert_shape_to_delta(URL_SHAPE, UC_DELTA_TABLE)


# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 200;
# MAGIC
# MAGIC CREATE or REPLACE TABLE amazonia.geo.silver_h3_desmatamento
# MAGIC AS
# MAGIC (
# MAGIC     SELECT *, explode(h3_polyfillash3(geometry, 10)) as h3_10_id
# MAGIC     FROM amazonia.geo.bronze_shp_desmatamento
# MAGIC );
