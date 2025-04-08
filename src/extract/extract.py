from common.libs.spark import *
from model.sellers_model import *
from utils.general.extract_helper import *
from parameters.sellers_parameters import sellers_params, PRODUCTS_LIMIT

def main():


    spark = create_spark_session()

    seller_instances = get_sellers(sellers_params)
    get_sellers_products(spark, sellers=seller_instances, limit=PRODUCTS_LIMIT, fetch_categories=True)

    ## Bronzes
    ingest_products_table(spark, seller_instances)
    ingest_sellers_table(spark, seller_instances)

    

if __name__ == '__main__':
    main()







