
WAREHOUSE_LOCATION_PARAM = "storage" # Leave empty to use default warehouse path.
CSV_PATH = "" # Leave empty to use default CSV path.
PARQUET_PATH = "" # Leave empty to use default Parquet path.



# DATABASE
DATABASE_NAME = "brl_prod_prices" 

# BRONZE
BRONZE_PRODUCTS_TABLE = "b_products"
BRONZE_SELLERS_TABLE = "b_sellers"

# SILVER
SILVER_CATALOG_PRODUCTS_TABLE = "s_catalog_products"
SILVER_LAST_VER_PRODUCTS_TABLE = "s_last_ver_products"
SILVER_CATEGORIES_TABLE = "s_categories_catalog"


METASTORE_INFO = {
    "DATABASE_NAME": "brl_prod_prices",

    "TABLES": {
        "BRONZE": {
            "BRONZE_PRODUCTS_TABLE": "b_products",
            "BRONZE_SELLERS_TABLE" : "b_sellers",
        },

        "SILVER": { 
            "SILVER_CATALOG_PRODUCTS_TABLE" : "s_catalog_products",
            "SILVER_LAST_VER_PRODUCTS_TABLE" : "s_last_ver_products",
            "SILVER_CATEGORIES_TABLE" : "s_categories_catalog",
        },
    }
}

