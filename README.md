# brl-prod-prices

Um ETL de sites de e-commerce brasileiros.

## Sellers

### Lista de sellers lidos

**Atualizado em -** ```07/04/2025```
- Zoom
- Magazine Luiza
- Mercado Livre


## Setup/Build

1. Clone o repositório:

```bash
git clone https://github.com/leoaugustosv/brl-prod-prices.git
cd brl-prod-prices
```

2. Crie as imagens dos containers:

```bash
docker-compose build
```

ou

```bash
docker-compose up persistence extractor transformer --build
```

Se quiser buildar e executar todos os containers de uma vez.



## Objetivos

<details>
<summary> <b><u>Objetivos previstos e cumpridos</u> (clique para expandir)</b>  </summary> 
<br/>

**Atualizado em -** ```11/04/2025```

1. Extrair informações variadas dos produtos mais vendidos em cada categoria de diferentes e-commerces brasileiros - **```(100%)```**. ✅
2. Transformar as informações extraídas em dataframes com dados relevantes [nome, preço, url, avaliações, etc] - **```(100%)```**. ✅
3. Ingestar os dataframes obtidos em tabelas delta locais bronze, particionadas por data [Products, Sellers] - **```(100%)```**. ✅
4. Possibilitar export de partições das tabelas delta para arquivos únicos [ATUALMENTE: CSV, Parquet] - **```(100%)```**. ✅
5. Ingestar tabela silver com um id único para cada produto, usando a chave URL - **```(100%)```**. ✅
6. Ingestar tabela silver com a última versão de cada produto por partição, usando a chave URL - **```(100%)```**. ✅
7. Ingestar tabela silver com a última versão de cada categoria, usando a chave URL - **```(100%)```**. ✅
8. Criar imagens para executar o ETL em diferentes containers Docker (armazenamento, extract, transform) - **```(100%)```**. ✅
9. Otimizar scraping para lidar com poucos recursos e OS instável - **```(100%)```**. ✅
10. Integrar projeto com a Cloud (GCP) - **(0%)**. ❌
- **OBS:** Ainda analisando como será feito e com quais objetivos.
11. Integrar ao projeto um modelo de aprendizado não supervisionado usando o algoritmo KMeans com os dados de produtos - **(0%)**. ❌
- **OBS:** Ainda analisando como será feito e com quais objetivos.

</details>

## Dependências

### Java

- JDK 17

### Python

- Selenium
- BeautifulSoup4 (bs4)
- PySpark
- Pandas
- delta-spark


### Opcional

- ipykernel (Fornece kernel para executar notebooks Jupyter)

Demais dependências atualizadas constam no arquivo ```requirements.txt```.

## Docker

<details>
<summary> <b><u>Informação dos containers</u> (clique para expandir)</b>  </summary> 

### Persistence

- **Função:** Armazena os DBs e suas respectivas tabelas delta.


### Extractor

- **Função:** Extrai via ChromeDriver os dados de produtos e sellers.
- **Porta:** 5900

### Transformer

- **Função:** Transforma os dados da camada bronze e salva na camada silver.


</details>



## Tabelas

<details>
<summary> <b><u>Documentação de tabelas</u> (clique para expandir)</b>  </summary> 

As tabelas são salvas utilizando delta-spark, criando-se na raíz do repositório um armazenamento Hive Metastore.


### Camada Bronze (b_prod_prices)

#### Produtos (b_prod_prices.b_products)

```Tabela de origem dos produtos extraídos de cada seller.```

| Nome | Tipo | Descrição | Default | Origem
| --- | --- | --- | --- | ---
| id | string | Id único da ingestão de cada produto | - | Gerado ao ser ingestado
| name | string | Nome do produto | - | Web scraping
| url | string | Link para a página do produto | - | Web scraping
| category | string | Categoria definida pelo próprio e-commerce | - | Web scraping
| price_in_cash | double | Preço à vista | - | Web scraping
| price_in_installments | double | Preço parcelado | -999 | Web scraping ou  calculado
| installments_num | int | Número de parcelas possíveis | 0 | Web scraping
| installments_value | double | Valor da parcela | -999 | Web scraping ou calculado
| img | string | Link para a imagem do produto | null | Web scraping
| seller | string | Nome do e-commerce em que o produto foi extraído | - | Atribuído automaticamente
| rating | double | Nota de avaliação do produto | -999 | Web scraping
| rating_users | int | Quantidade de usuários que avaliaram o produto | 0 | Web scraping
| position | int | Posição do produto nos mais vendidos da categoria em que se encontra | - | Gerado ao ser ingestado
| dt_refe_crga | string | Data da ingestão | - | Gerado em tempo de carga
| dh_exec | timestamp | Data e hora da ingestão | - | Gerado em tempo de carga

**Características**

- **Chaves**: url + dh_exec
- **Schema**: MergeSchema
- **Modo de carga**: Append
- **Partição**: dt_refe_crga

----

#### Sellers (b_prod_prices.b_sellers)

```Tabela de origem dos sellers parametrizados e suas respectivas categorias obtidas.```

| Nome | Tipo | Descrição | Default | Origem
| --- | --- | --- | --- | ---
| id | string | Id único do vendedor | - | Arquivo de parâmetros
| name | string | Nome do vendedor | - | Arquivo de parâmetros
| url | array(array(string)) | Links específicos para extração dos produtos e categorias do vendedor | - | Arquivo de parâmetros
| categories | array(map(string,string)) | Categoria definida pelo próprio e-commerce | - | Web scraping
| dt_refe_crga | string | Data da ingestão | - | Gerado em tempo de carga
| dh_exec | timestamp | Data e hora da ingestão | - | Gerado em tempo de carga

**Características**

- **Chaves**: id + dh_exec
- **Schema**: MergeSchema
- **Modo de carga**: Overwrite (partição)
- **Partição**: dt_refe_crga

----

### Silver (s_last_ver_products_table)

```Tabela visão produto. Exibe sempre a última versão de cada produto e suas respectivas informações.```

| Nome | Tipo | Descrição | Default | Origem
| --- | --- | --- | --- | ---
| DS_URL | string | URL do produto | - | Gerado na origem
| ID_ORIGIN | string | Id de ingestão do produto | - | Gerado na origem
| NM_PRODUCT | string | Nome do produto | - | Gerado na origem
| DS_IMG | string | Categoria definida pelo próprio e-commerce | - | Gerado na origem
| DS_CATEGORY | string | Categoria definida pelo próprio e-commerce | - | Gerado na origem
| VL_CASH | double | Preço à vista | - | Gerado na origem
| VL_INSTALLMENTS | double | Preço parcelado | - | Gerado na origem
| NR_INSTALLMENTS | int | Número de parcelas possíveis | - | Gerado na origem
| VL_SINGLE_INSTALLMENT | double | Valor da parcela | - | Gerado na origem
| NM_SELLER | string | Nome do e-commerce em que o produto foi extraído | - | Gerado na origem
| VL_RATING | double | Nota de avaliação do produto | - | Gerado na origem
| QT_RATING_USERS | int | Quantidade de usuários que avaliaram o produto | - | Gerado na origem
| NR_POSITION | int | Posição do produto nos mais vendidos da categoria em que se encontra | - | Gerado na origem
| dt_refe_crga | string | Data da ingestão | - | Gerado em tempo de carga
| dh_exec | timestamp | Data e hora da ingestão | - | Gerado em tempo de carga

**Características**

- **Chaves**: ID_CATALOG
- **Schema**: MergeSchema
- **Modo de carga**: Overwrite (partição)
- **Partição**: dt_refe_crga
- **Origem**: b_products
- **Tipos de carga**: Full

----


### Silver (s_catalog_products)

```Tabela visão produto sumarizada. Define um uuid para cada produto já ingestado em algum momento na origem.```

| Nome | Tipo | Descrição | Default | Origem
| --- | --- | --- | --- | ---
| ID_ORIGIN | string | Id de ingestão do produto | - | Gerado na origem
| NM_PRODUCT | string | Nome do produto | - | Gerado na origem
| DS_URL | string | URL do produto | - | Gerado na origem
| DS_IMG | string | URL da imagem do produto | - | Gerado na origem
| ID_CATALOG | string | UUID único do produto | - | Gerado em tempo de carga
| dt_refe_crga | string | Data da ingestão | - | Gerado em tempo de carga
| dh_exec | timestamp | Data e hora da ingestão | - | Gerado em tempo de carga

**Características**

- **Chaves**: ID_CATALOG
- **Schema**: MergeSchema
- **Modo de carga**: Overwrite (partição)
- **Partição**: dt_refe_crga
- **Origem**: b_products
- **Tipos de carga**: Full

----

### Silver (s_catalog_products)

```Tabela visão categoria-seller. Criada a partir das estruturas de categorias de cada seller na origem.```

| Nome | Tipo | Descrição | Default | Origem
| --- | --- | --- | --- | ---
| ID_CATEGORY | string | Id sequencial único para cada categoria | - | Gerado em tempo de carga
| DS_CATEGORY | string | Descrição da categoria | - | Gerado na origem
| NM_SELLER | string | Nome do e-commerce em que o produto foi extraído | - | Gerado na origem
| DS_URL | string | URL da categoria | - | Gerado na origem
| DH_ORIGIN_EXEC | timestamp | Data e hora da última ingestão da categoria na origem | - | Gerado na origem
| dt_refe_crga | string | Data da ingestão | - | Gerado em tempo de carga
| dh_exec | timestamp | Data e hora da ingestão | - | Gerado em tempo de carga

**Características**

- **Chaves**: ID_CATEGORY
- **Schema**: MergeSchema
- **Modo de carga**: Overwrite (partição)
- **Partição**: dt_refe_crga
- **Origem**: b_products
- **Tipos de carga**: 
    - **Full** (total)
    - **Incremental** (comparativo entre última partição salva VS tempo de execução)
    
----



</details>