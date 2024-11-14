<img src='https://github.com/Databricks-BR/amazonia_geoai/raw/main/images/logo_amazonia_geo_ai.png'></img></a>

## CONTEXTO

MAIOR BIOMA DO PLANETA, a **Amazônia** enfrenta desafios complexos e multifacetados que exigem soluções inovadoras e urgentes. </br>

O desmatamento desenfreado, a degradação ambiental, as mudanças climáticas e a crise social colocam em risco a integridade do ecossistema amazônico, o futuro das comunidades que dependem dele e o de todos nós.

</br></br>

## AUTORES


## RELEVÂNCIA DO TEMA

É um tema atual e de grande interesse mundial. Relevante também para as grandes empresas listadas na Bolsa B3, nos temas de ESG e Crédito de Carbono.
</br></br>
A empresa **VALE**, por exemplo, tem um programa chamado “Meta Florestal 2030”, que é um compromisso voluntário e ambicioso estabelecido em 2019, que visa proteger e recuperar 500 mil hectares de florestas no Brasil até 2030.  Deste total, 400 mil hectares serão destinados à proteção de florestas existentes, e 100 mil hectares serão recuperados por meio de sistemas sustentáveis. Este objetivo está alinhado com a Agenda 2030 da ONU e contribui para a meta da Vale de se tornar neutra em carbono até 2050.
</br></br>
A empresa **NATURA**, tem no site de Relacionamento com Investidores, sua Meta de ESG declarada, de “Proteger a Amazônia", através da expansão da influência na conservação de 1,8 milhão de hectares, em 33 comunidades, para 3 milhões hectares, em 40 comunidades, incentivando esforços coletivos com relação ao desmatamento zero até 2025.

</br></br>

## PROPÓSITO - Proposta de Solução

Criar uma solução inovadora para Análise e Monitoramento Geo Espacial, dentro do tema ambiental,  dimensionando, através da correlação de dados de múltiplas fontes (mapas, imagens de satélite das queimada, limites geográficos das regiões, dados geográficos, etc), a REAL AMPLITUDE dos impactos ambientais das Queimadas e Desmatamentos no bioma da Amazônia, utilizando Modelos de Inteligência Artificial para segmentar e classificar as ocorrências por risco, gerando SCOREs de criticidade e impacto ambiental, em cada MICRORREGIÃO, indexada por funções GEO H3, criando um índice único e uma visão 360 de todas as informações da região mapeada.

</br></br>

## INSTRUÇÕES para execução

1. Criar uma Git Folder no seu workspace do Databricks apontando para este diretório
2. Executar o notebook `databricks_notebooks/Amazonia_Geo_AI_v3 - Ingestion` para ingerir os dados nas tabelas bronze e transformá-los em índices H3 na camada silver
3. Criar um Databricks Apps do tipo "Custom", utilizando os arquivos da pasta `databricks-apps` como "source"

</br></br>


## ARQUITETURA

</br></br>

## CRIATIVIDADE

</br></br>

## REFERÊNCIAS

### Referências Técnicas

* [Shapefile com Geopandas - sample code](https://github.com/databrickslabs/mosaic/blob/main/notebooks/examples/python/Shapefiles/GeoPandasUDF/shapefiles_geopandas_udf.ipynb)
* [Pydeck e DECK.gl](https://medium.com/vis-gl/pydeck-unlocking-deck-gl-for-use-in-python-ce891532f986)
* [Geo Mosaic com Sedona - sample code](https://github.com/databrickslabs/mosaic/blob/main/notebooks/examples/python/Sedona/MosaicAndSedona.ipynb)
* [Tabela com tamanhos do Hexagono H3](https://h3geo.org/docs/core-library/restable)
* [Kepler.gl - sample code](https://github.com/keplergl/kepler.gl/tree/master/docs/keplergl-jupyter)
* [Kepler.gl - geojson layer](https://deck.gl/examples/geojson-layer-paths)
* [Pydeck.gl - Blog Medium](https://medium.com/vis-gl/pydeck-unlocking-deck-gl-for-use-in-python-ce891532f986)
* [Deck.gl - mapa H3](https://deckgl.readthedocs.io/en/latest/gallery/h3_hexagon_layer.html)
* [Geospatial Databricks - BLOG](https://www.databricks.com/blog/2019/12/05/processing-geospatial-data-at-scale-with-databricks.html)
* [Deck.gl - mapa H3 - github](https://github.com/visgl/deck.gl/blob/master/examples/gallery/src/hexagon-layer.html)
* [Geospatial Functions H3](https://docs.databricks.com/en/sql/language-manual/sql-ref-h3-geospatial-functions-examples.html)
* [Geospatial Functions H3 - Polly Fill as H3](https://docs.databricks.com/en/sql/language-manual/functions/h3_polyfillash3.html)


### Download Shapefile e Text Files CSV

* https://terrabrasilis.dpi.inpe.br/sobre/
* https://terrabrasilis.dpi.inpe.br/downloads/
* https://terrabrasilis.dpi.inpe.br/queimadas/portal/
* https://dataserver-coids.inpe.br/ccst/p4cn/4%20Inventario-CN/%C3%81reas%20protegidas/
* https://dataserver-coids.inpe.br/queimadas/queimadas/area_queimada/AQ1km/shp/
* https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/
* https://www.ibge.gov.br/estatisticas/multidominio/genero/22827-censo-demografico-2022.html?edicao=39499&t=resultados


