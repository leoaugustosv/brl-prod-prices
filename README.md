# brl-prod-prices

Um ETL de sites de e-commerce brasileiros.

## Sellers

### Lista de sellers lidos (updated: 07/04/25)
- Zoom
- Magazine Luiza
- Mercado Livre

## Objetivos

1. Extrair informações variadas dos produtos mais vendidos em cada categoria de diferentes e-commerces brasileiros - **```(100%)```**.

2. Transformar as informações extraídas em dataframes com dados relevantes [nome, preço, url, avaliações, etc] - **```(100%)```**.
3. Ingestar os dataframes obtidos em tabelas delta locais bronze, particionadas por data [Products, Sellers] - **```(100%)```**.
4. Possibilitar export de partições das tabelas delta para arquivos únicos [ATUALMENTE: CSV, Parquet] - **```(100%)```**.
5. Ingestar tabela silver com um id único para cada produto, usando a chave URL - **```(100%)```**.
6. Ingestar tabela silver com a última versão de cada produto por partição, usando a chave URL - **```(100%)```**.
7. Ingestar tabela silver com a última versão de cada categoria, usando a chave URL - **```(100%)```**.
8. Criar imagem para executar o ETL em um container Docker - **(0%)**.
9. Integrar projeto para realizar carga das silvers criadas para a Cloud (GCP) - **(0%)**.
10. Integrar ao projeto um modelo de aprendizado não supervisionado usando o algoritmo KMeans com os dados de produtos - **(0%)**.

## Dependências

- Selenium
- BeautifulSoup4 (bs4)
- PySpark
- Pandas
- delta-spark

### Opcional

- ipykernel (Fornece kernel para executar notebooks Jupyter)

Demais dependências atualizadas constam no arquivo ```requirements.txt```.

## Características

### Pros

- **Extração assíncrona**: cada seller é extraído em sua própria janela, sem sincronismo com os demais.
- **Simulação de ação humana**: atua extraindo dados evitando que o script seja percebido pelos sellers como uma automação.
- **Persistência local**: persiste os dados extraídos em tabelas delta em um hive metastore criado localmente.
- **Exportar dados para arquivos**: permite exportar partições das tabelas delta para arquivos (CSV, Parquet).
- **Dinamismo para acrescentar novos sellers**: a parametrização de novos sellers é similar.
- **Gera dados reais**: gera dados reais a partir da interação de consumidores com os sellers parametrizados, permitindo análises ao longo do tempo dos produtos extraídos.


### Cons

- **Requer manutenção na extração:** Novas mudanças na estrutura do site de cada seller podem exigir manutenção na forma como os dados são capturados e calculados.
- **Velocidade:** Por simular a ação humana de navegar por cada categoria, aumenta exponencialmente o tempo para extrair os dados conforme o número de categorias que um seller possui.
- **Processo local**: Por rodar localmente (por enquanto), carece de alta disponibilidade, escalabilidade e outras vantagens que uma núvem pública oferece.

---
<details>
<summary> <b><u>Documentação de tabelas</u> (clique para expandir)</b>  </summary> 

## Tabelas

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