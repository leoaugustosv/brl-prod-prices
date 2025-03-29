
from model.sellers_model import *
from libs.spark import *
from helpers.functions import *
from parameters.sellers_parameters import sellers_params
from parameters.general_parameters import PRODUCTS_LIMIT




def main():


    spark = create_spark_session()
    # browser = sl.init_browser()

    seller_instances = get_sellers(sellers_params)
    get_sellers_products(spark, sellers=seller_instances, limit=PRODUCTS_LIMIT, fetch_categories=True)
    
    
    ingest_products_table(spark, seller_instances)

    ingest_sellers_table(spark, seller_instances)

    

if __name__ == '__main__':
    main()







