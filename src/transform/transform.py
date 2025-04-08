
from common.libs.spark import *
from common.parameters.common_params import *
from utils.general.transform_helper import *


def main():


    spark = create_spark_session()

    ## Silvers
    ingest_silver_products_catalog(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_last_ver_products(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_categories_catalog(spark, bronze_path=f"{DATABASE_NAME}.{BRONZE_SELLERS_TABLE}")

    

if __name__ == '__main__':
    main()







