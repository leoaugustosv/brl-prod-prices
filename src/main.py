
from model.sellers_model import *
from libs.spark import *
from helpers.functions import *
from parameters.sellers_parameters import sellers_params
from parameters.general_parameters import PRODUCTS_LIMIT




def main():


    spark = create_spark_session()

    seller_instances = get_sellers(sellers_params)
    get_sellers_products(spark, sellers=seller_instances, limit=PRODUCTS_LIMIT, fetch_categories=True)

    ## Bronzes
    ingest_products_table(spark, seller_instances)
    ingest_sellers_table(spark, seller_instances)

    ## Silvers
    ingest_silver_products_catalog(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_last_ver_products(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_categories_catalog(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_SELLERS_TABLE}")
    

if __name__ == '__main__':
    main()







