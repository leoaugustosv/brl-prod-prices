# brl-prod-prices

Um ETL de sites de e-commerce brasileiros.

## Objetivos

1. Extrair informações variadas dos produtos mais vendidos em cada categoria de diferentes e-commerces brasileiros - **```(100%)```**.

2. Transformar as informações extraídas em dataframes com dados relevantes [nome, preço, url, avaliações, etc] - **```(100%)```**.
3. Ingestar os dataframes obtidos em tabelas delta locais bronze, particionadas por data [Products, Sellers] - **```(100%)```**.
4. Possibilitar export de partições das tabelas delta para arquivos únicos [ATUALMENTE: CSV, Parquet] - **```(100%)```**.
5. Ingestar tabelas silver com a última versão de cada produto por partição, usando a chave URL - **(0%)**.
6. Criar imagem para executar o ETL em um container Docker - **(0%)**.
7. Integrar projeto para realizar carga das silvers criadas para a Cloud (AWS ou Azure) - **(0%)**.
8. Integrar ao projeto um modelo de aprendizado não supervisionado usando o algoritmo KMeans com os dados de produtos - **(0%)**.

## Dependências

- Selenium
- BeautifulSoup4 (bs4)
- PySpark
- Pandas
- delta-spark

Demais dependências atualizadas constam no arquivo ```requirements.txt```.