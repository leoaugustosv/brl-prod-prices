from common.libs.spark import *

from common.parameters.common_params import *
from common.libs.spark import *

from model.sellers_model import *
import libs.selenium as sl

from utils.sellers import zoom as z
from utils.sellers import magalu as mgl
from utils.sellers import mercadolivre as meli

from concurrent.futures import ThreadPoolExecutor
import functools

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
import pyspark.sql.functions as F


def get_sellers(sellers_list):

    sellers = []

    print("\n--- Getting active sellers infos...")
    for seller in sellers_list:
        
        curr_seller_name = seller.get("name")
        curr_seller_urls = seller.get("url")
        curr_seller_id = seller.get("id")
        curr_seller_categories = seller.get("categories")
        curr_seller_active = seller.get("active")

        if curr_seller_active:
            sellers.append(
                Seller(
                    id=curr_seller_id,
                    name=curr_seller_name,
                    url=curr_seller_urls,
                    categories=curr_seller_categories,
                    active=curr_seller_active
                    )
                )

    
    if len(sellers) == 0:
        print("\n---No active sellers found. Please adjust the parameter file accordingly and try again.")
    else:
        print("\n--- Active sellers found:")
        for seller in sellers:
            seller_category_count = len(seller.categories)
            seller_url_count = len(seller.url)
            print(f"- {seller.name} ({seller_url_count} urls, {seller_category_count} categories)")

    return sellers


def fetch_sellers_categories(sellers):
    with ThreadPoolExecutor(max_workers=len(sellers)) as executor:
        executor.map(fetch_categories_for_seller, sellers)


def fetch_categories_for_seller(seller):

    browser = sl.init_browser()

    category_url = ""
    homepage_url = ""

    for url in seller.url:
        if url[1] == 1:
            category_url = url[0]
        if url[1] == 0:
            homepage_url = url[0]
    
    if not category_url:
        print(f"\n---No specific category URL found on parameters for {seller.name}. Using homepage as category URL...")
        category_url = homepage_url
    
    if seller.name == "Zoom":
        seller.categories.extend(z.get_html_categories(browser, category_url))
    elif seller.name == "Magalu":
        seller.categories.extend(mgl.get_html_categories(browser, category_url))
    elif seller.name == "MercadoLivre":
        seller.categories.extend(meli.get_html_categories(browser, category_url))

    sl.close_browser(browser=browser)

    print(f"---Categories found for {seller.name}: {len(seller.categories)}")


        

def get_sellers_products(spark, sellers, limit, fetch_categories = True):

    if fetch_categories:
        fetch_sellers_categories(sellers)

    last_product_df = read_table(spark, f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}", last_part_only=True, return_empty_df_if_missing=True)

    if last_product_df.isEmpty():
        print(f"There is no Product ID yet. Considering last Product ID as 0.")
        last_product_id = 0
    else:
        last_product_df = last_product_df.selectExpr("id").withColumn("id", F.col("id").cast('int')).orderBy(F.col("id").desc())
        last_product_id = int(last_product_df.collect()[0][0])
        print(f"Last Product ID found on table: {last_product_id}")
   
    # Partial: to allow executor map passing parameters to function
    partial_products_fetch = functools.partial(fetch_products_for_seller, last_product_id, limit)

    with ThreadPoolExecutor(max_workers=len(sellers)) as executor:
        executor.map(partial_products_fetch, sellers)





def fetch_products_for_seller(last_product_id, limit, seller):

    browser = sl.init_browser()
    
    if len(seller.categories) == 0:
        print(f"\n---Skipping {seller.name}, as no category URL was found.")

    else:
        try:

            if seller.name == "Zoom":
                last_product_id, seller.products = z.get_product_prices(browser, seller.categories, last_product_id, limit)
            elif seller.name == "Magalu":
                last_product_id, seller.products = mgl.get_product_prices(browser, seller.categories, last_product_id, limit)
            elif seller.name == "MercadoLivre":
                last_product_id, seller.products = meli.get_product_prices(browser, seller.categories, last_product_id, limit)

        except Exception as e:
            print(f"ERROR: {e}")
        
        print(f"---Products found for {seller.name}: {len(seller.products)}")

    sl.close_browser(browser=browser)



def ingest_products_table(spark, sellers):


    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price_in_cash", DoubleType(), True),
        StructField("price_in_installments", DoubleType(), True),
        StructField("installments_num", IntegerType(), True),
        StructField("installment_value", DoubleType(), True),
        StructField("img", StringType(), True),
        StructField("seller", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("rating_users", IntegerType(), True),
        StructField("position", IntegerType(), True),
    ])

    products_data = []
    for seller in sellers:
        for product in seller.products:
            product_dict = {
                "name":product.name,
                "id" :product.id,
                "url" :product.url,
                "category" :product.category,
                "price_in_cash" :product.price_in_cash,
                "price_in_installments" :product.price_in_installments,
                "installments_num" :product.installments_num,
                "installment_value" :product.installment_value,
                "img" :product.img,
                "seller" :product.seller,
                "rating" :product.rating,
                "rating_users" :product.rating_users,
                "position" :product.position,
            }
            products_data.append(product_dict)
    
    df = (
        spark.createDataFrame(products_data, schema=schema)
        .orderBy(F.col("id").desc())
        .distinct()
        .withColumn("dt_refe_crga", F.current_date().cast('string'))
        .withColumn("dh_exec", F.current_timestamp())
        )
    
    df.show()

    try:
        save_table(spark, df, path=f"{DATABASE_NAME}.{BRONZE_PRODUCTS_TABLE}", partition_column="dt_refe_crga", mode="append", schema_option="merge")
    except Exception as e:
        print("ERROR", e)

    return df



def ingest_sellers_table(spark, sellers):



    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("url", ArrayType(ArrayType(StringType())), True),
        StructField("categories", ArrayType(MapType(StringType(), StringType())), True)
    ])

    sellers_data = []
    for seller in sellers:
        seller_dict = {
            "name":seller.name,
            "id" :seller.id,
            "url" :seller.url,
            "categories" :seller.categories
        }
        sellers_data.append(seller_dict)
        
    df = (
        spark.createDataFrame(sellers_data, schema=schema)
        .withColumn("dt_refe_crga", F.current_date().cast('string'))
        .withColumn("dh_exec", F.current_timestamp())
        )
    
    df.show()
    
    try:
        save_table(spark, df, path=f"{DATABASE_NAME}.{BRONZE_SELLERS_TABLE}", partition_column="dt_refe_crga", mode="overwrite", schema_option="merge")
    except Exception as e:
        print("ERROR: ", e)

    return df

