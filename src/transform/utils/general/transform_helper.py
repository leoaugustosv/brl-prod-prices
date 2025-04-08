from common.parameters.common_params import *
from common.libs.spark import *

import functools
from concurrent.futures import ThreadPoolExecutor

from datetime import date
import uuid

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
import pyspark.sql.functions as F
import pyspark.sql.window as W


def ingest_silver_products_catalog(spark, bronze_path):

    # informative
    schema = StructType([
        StructField("ID_ORIGIN", StringType(), False),
        StructField("NM_PRODUCT", StringType(), True),
        StructField("DS_URL", StringType(), True),
        StructField("DS_IMG", StringType(), True),
        StructField("ID_CATALOG", StringType(), True),
    ])

    bronze_df = read_table(spark, bronze_path, last_part_only=False, return_empty_df_if_missing=True)

    if bronze_df.isEmpty():
        print(f"SILVER: No partition found for table {bronze_path}. Aborting products catalog silver ingestion...")
        return
    else:
        try:
            window_spec = W.Window.partitionBy(F.col("url")).orderBy(F.col("dh_exec").desc())


            silver_df = (
                bronze_df
                .withColumn("rank", F.row_number().over(window_spec))
                .filter(F.col("rank") == 1)
                .selectExpr(
                    "id as ID_ORIGIN",
                    "name as NM_PRODUCT",
                    "seller as NM_SELLER",
                    "url as DS_URL",
                    "img as DS_IMG"
                )
                .withColumn("ID_CATALOG", F.concat_ws("-",F.substring(F.col("NM_SELLER"), 1, 4), F.expr("uuid()")))
                .withColumn("dt_refe_crga", F.current_date().cast('string'))
                .withColumn("dh_exec", F.current_timestamp())
            )

            silver_df.show()

            save_table(spark, silver_df, path=f"{DATABASE_NAME}.{SILVER_CATALOG_PRODUCTS_TABLE}", partition_column="dt_refe_crga", mode="overwrite", schema_option="merge")
        except Exception as e:
            print("ERROR: ", e)
            
    return silver_df

def ingest_silver_last_ver_products(spark, bronze_path, ignore_delay = False):

    df = read_table(spark, bronze_path, return_empty_df_if_missing=True)
    

    if df.isEmpty():
        print(f"SILVER: No partition found for table {bronze_path}. Aborting unique products silver ingestion...")
        return
    
    else:
        try:
            window_spec = W.Window.partitionBy(F.col("url")).orderBy(F.col("dh_exec").desc())

            part_name = spark.sql(f"DESCRIBE DETAIL {f'{bronze_path}'}").selectExpr("partitionColumns").collect()[0][0][0]
            last_parts = spark.read.table(bronze_path).selectExpr(f"max({part_name})").collect()[0]


        
            if ignore_delay:
                aim_partition = last_parts[0]
            else:
                ### By default reading with minimum D-1 delay
                today = str(date.today())
                if last_parts[0] == today and len(last_parts) > 1:
                    aim_partition = last_parts[1]
                else:
                    aim_partition = last_parts[0]

            print(f"SILVER: Reading from bronze products table partition {aim_partition}...")

            catalog_df = (
                read_table(spark, path=f"{DATABASE_NAME}.{SILVER_CATALOG_PRODUCTS_TABLE}" ,last_part_only=True, return_empty_df_if_missing=True)
                .selectExpr(
                    "ID_CATALOG",
                    "DS_URL"
                )
                .distinct()
            )

            silver_df = (
                df
                .filter(F.col(part_name) == aim_partition)
                .withColumn("rank", F.row_number().over(window_spec))
                .filter(F.col("rank") == 1)
                .selectExpr(
                    "id as ID_ORIGIN",
                    "name as NM_PRODUCT",
                    "url as DS_URL",
                    "img as DS_IMG",
                    "category as DS_CATEGORY",
                    "price_in_cash as VL_CASH",
                    "price_in_installments as VL_INSTALLMENTS",
                    "installments_num as NR_INSTALLMENTS",
                    "installment_value as VL_SINGLE_INSTALLMENT",
                    "seller as NM_SELLER",
                    "rating as VL_RATING" ,
                    "rating_users as QT_RATING_USERS",
                    "position as NR_POSITION",                    
                )
            )

            if catalog_df.isEmpty():
                silver_df = (
                    silver_df.withColumn("ID_CATALOG", F.lit(None).cast("string"))
                )
            else:
                silver_df = (
                    silver_df.join(catalog_df, "DS_URL", "left")
                )

            silver_df = (
                silver_df
                .withColumn("dt_refe_crga", F.current_date().cast('string'))
                .withColumn("dh_exec", F.current_timestamp())
            )
            

            silver_df.show()

            save_table(spark, silver_df, path=f"{DATABASE_NAME}.{SILVER_LAST_VER_PRODUCTS_TABLE}", partition_column="dt_refe_crga", mode="overwrite", schema_option="merge")
        except Exception as e:
            print("ERROR: ", e)

        return silver_df
    


def ingest_silver_categories_catalog(spark, bronze_path, ignore_delay = False, load_type = None):

    id_window_spec = W.Window.orderBy(F.concat_ws("|",F.col("DS_CATEGORY"),F.col("NM_SELLER")))
    last_ver_window_spec = W.Window.partitionBy(F.col("id")).orderBy(F.col("dh_exec").desc())
    
    existing_silver_df = (read_table(spark, f"{DATABASE_NAME}.{SILVER_CATEGORIES_TABLE}", return_empty_df_if_missing=True, last_part_only=True))
    

    if not load_type or load_type not in ("full", "incremental"):
        load_type = "full" if existing_silver_df.isEmpty() else "incremental"

    
    if load_type == "full":
        try:
            
            df = read_table(spark, bronze_path, return_empty_df_if_missing=True, last_part_only=False)

            if df.isEmpty():
                print(f"SILVER: No partition found for table {bronze_path}. Aborting categories silver ingestion...")
                return

            print(f"SILVER: FULL LOAD - Reading all bronze sellers table partitions. All existing IDs (if any) will be overridden...")

            silver_df = (
                df
                .withColumn("rank", F.row_number().over(last_ver_window_spec))
                .filter(F.col("rank") == 1)
                .select(
                    F.col("name").alias("NM_SELLER"),
                    F.col("dh_exec").alias("DH_ORIGIN_EXEC"),
                    F.explode("categories").alias("exploded")
                )
                .withColumn("DS_CATEGORY", F.expr("map_keys(exploded)[0]"))
                .withColumn("DS_URL", F.expr("map_values(exploded)[0]"))
                .drop("exploded")
                .withColumn("DS_CATEGORY", F.trim(F.regexp_replace(F.col("DS_CATEGORY"), r"\+\s*", "")))
                .distinct()
                .withColumn("ID_CATEGORY", F.row_number().over(id_window_spec))
                .select(
                    "ID_CATEGORY",
                    "DS_CATEGORY",
                    "NM_SELLER",
                    "DS_URL",
                    "DH_ORIGIN_EXEC",
                )
            )

            silver_df = (
                silver_df
                .withColumn("dt_refe_crga", F.current_date().cast('string'))
                .withColumn("dh_exec", F.current_timestamp())
            )
        
            save_table(spark, silver_df, path=f"{DATABASE_NAME}.{SILVER_CATEGORIES_TABLE}", partition_column="dt_refe_crga", mode="overwrite", schema_option="merge")
        except Exception as e:
            print("ERROR: ", e)


    elif load_type=="incremental":

        try:
            df = read_table(spark, bronze_path, return_empty_df_if_missing=True, last_part_only=False)

            part_name = spark.sql(f"DESCRIBE DETAIL {f'{bronze_path}'}").selectExpr("partitionColumns").collect()[0][0][0]
            last_parts = spark.read.table(bronze_path).selectExpr(f"max({part_name})").collect()[0]


            if ignore_delay:
                aim_partition = last_parts[0]
            else:
                ### By default reading with minimum D-1 delay
                today = str(date.today())
                if last_parts[0] == today and len(last_parts) > 1:
                    aim_partition = last_parts[1]
                else:
                    aim_partition = last_parts[0]

            print(f"SILVER: INCREMENTAL LOAD - Reading from bronze sellers table partition {aim_partition}...")


            curr_exec = (
                df
                .filter(F.col(part_name) == aim_partition)
                .withColumn("rank", F.row_number().over(last_ver_window_spec))
                .filter(F.col("rank") == 1)
                .select(
                    F.col("name").alias("NM_SELLER"),
                    F.col("dh_exec").alias("DH_ORIGIN_EXEC"),
                    F.explode("categories").alias("exploded")
                )
                .withColumn("DS_CATEGORY", F.expr("map_keys(exploded)[0]"))
                .withColumn("DS_URL", F.expr("map_values(exploded)[0]"))
                .drop("exploded")
                .withColumn("DS_CATEGORY", F.trim(F.regexp_replace(F.col("DS_CATEGORY"), r"\+\s*", "")))
                .distinct()
                .select(
                    "DS_CATEGORY",
                    "NM_SELLER",
                    "DS_URL",
                    "DH_ORIGIN_EXEC",
                )
            )

            last_silver_category_id = existing_silver_df.selectExpr("max(CAST(ID_CATEGORY AS int))").collect()[0][0]

            if not last_silver_category_id:
                last_silver_category_id = 0

            silver_df = (
                existing_silver_df.alias('old')
                .join(curr_exec.alias('new'), ["DS_CATEGORY","NM_SELLER"], "full")
                .withColumn("id_cat_final", 
                    F.coalesce(
                        F.col("ID_CATEGORY"), 
                        F.lit(last_silver_category_id) + F.row_number().over(id_window_spec)
                    )
                )
                .selectExpr(
                    "id_cat_final as ID_CATEGORY",
                    "coalesce(new.DS_CATEGORY, old.DS_CATEGORY) as DS_CATEGORY",
                    "coalesce(new.NM_SELLER, old.NM_SELLER) as NM_SELLER",
                    "coalesce(new.DS_URL, old.DS_URL) as DS_URL",
                    "coalesce(new.DH_ORIGIN_EXEC, old.DH_ORIGIN_EXEC) as DH_ORIGIN_EXEC",
                )
            )

            silver_df = (
                    silver_df
                    .withColumn("dt_refe_crga", F.current_date().cast('string'))
                    .withColumn("dh_exec", F.current_timestamp())
                )
            
            save_table(spark, silver_df, path=f"{DATABASE_NAME}.{SILVER_CATEGORIES_TABLE}", partition_column="dt_refe_crga", mode="overwrite", schema_option="merge")
        except Exception as e:
            print("ERROR: ", e)

        return silver_df