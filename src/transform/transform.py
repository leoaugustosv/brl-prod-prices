
from common.libs.spark import *
from common.parameters.common_params import *
from utils.general.transform_helper import *


def main():

    BRONZE_DB_NAME = METASTORE_INFO["LAYERS"]["BRONZE"]["DATABASE_NAME"]
    BRONZE_PRODUCTS_TABLE = METASTORE_INFO["LAYERS"]["BRONZE"]["TABLES"]["BRONZE_PRODUCTS_TABLE"]
    BRONZE_SELLERS_TABLE = METASTORE_INFO["LAYERS"]["BRONZE"]["TABLES"]["BRONZE_SELLERS_TABLE"]

    spark = create_spark_session(layer="SILVER")

    ## Silvers
    ingest_silver_products_catalog(spark, bronze_path=f"{BRONZE_DB_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_last_ver_products(spark, bronze_path=f"{BRONZE_DB_NAME}.{BRONZE_PRODUCTS_TABLE}")
    ingest_silver_categories_catalog(spark, bronze_path=f"{BRONZE_DB_NAME}.{BRONZE_SELLERS_TABLE}")

    

if __name__ == '__main__':
    main()







