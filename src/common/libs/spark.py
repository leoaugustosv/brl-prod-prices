from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
from delta import configure_spark_with_delta_pip
from delta import DeltaTable as dt
from common.parameters.common_params import METASTORE_INFO, DATABASE_NAME, WAREHOUSE_LOCATION_PARAM, CSV_PATH, PARQUET_PATH

import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import os


def get_root_path(starting_path:str = None):
    if starting_path:
        current = starting_path
    else:
        current = os.getcwd()
        print("ROOT_PATH: Current path - ", current)

    return current
    


def create_spark_session(refresh_tb_locations = False, create_hive_db = True, app_name:str = "default") -> SparkSession:

    try: 
        # Setting WareHouse Folder
        if not WAREHOUSE_LOCATION_PARAM:
            warehouse_location = f'{get_root_path()}'
        else:
            warehouse_location = WAREHOUSE_LOCATION_PARAM
        warehouse_location = warehouse_location.replace("\\","\\\\")
        print(f"HIVE: Warehouse location: {warehouse_location}")
        
        if not os.path.exists(warehouse_location):
                print(f"HIVE: Warehouse location does not exists yet. Creating {warehouse_location} folder...")
                os.makedirs(warehouse_location)

    

        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.driver.memory", "8g") # Memory
            .config("spark.sql.session.timeZone", "-03:00") # TimeZone

            .config("spark.sql.warehouse.dir", warehouse_location) # Warehouse location
            .config('spark.driver.extraJavaOptions',f'-Dderby.system.home={warehouse_location}')
            
            # Enable Delta Spark
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            # Force Delta V1 Partitioning
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

            # Enable schema automerge
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

            # Disable appendOnly by default
            .config("spark.databricks.delta.properties.defaults.appendOnly", "false")

            #Enable Hive
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
        )

        spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        print(f"SPARK: Spark Session started. Spark Version: {spark_session.version}")
        
        # ERROR for supressing warnings, DEBUG to show warnings
        spark_session.sparkContext.setLogLevel("ERROR")

        # Creating database to persist infos in Hive Metastore
        if create_hive_db:
            create_database(spark_session, DATABASE_NAME, warehouse_location)
        
        if refresh_tb_locations:
            refresh_table_locations(spark_session, METASTORE_INFO, f"file:{warehouse_location}")

        return spark_session
    
    except Exception as e:
        print(f"SPARK_ERROR: {e}")



def read_table(spark, path, last_part_only=False, return_empty_df_if_missing = False):
    try:
        if last_part_only == True:

            part_name = spark.sql(f"DESCRIBE DETAIL {f'{path}'}").selectExpr("partitionColumns").collect()[0][0][0]
            last_part = spark.read.table(path).selectExpr(f"max({part_name})").collect()[0][0]
            
            df = (
                spark.read.table(path)
                .filter(F.col(part_name) == last_part)
            )

        else:
            df = (
                spark.read.table(path)
            )
    except AnalysisException as e:
        if return_empty_df_if_missing:
            print(f"TABLE_OPERATION: Table {path} not found. Returning empty dataframe...")
            df = spark.createDataFrame([], schema=StructType([]))
        else:
            raise AnalysisException(f"TABLE_OPERATION: AnalysisException ocurred ({e}). Consider enabling empty df return to deal specifically with this Exception.")
    except Exception as e:
        raise Exception(f"ERROR: {e}")
 
    return df


def read_unregistered_table(spark, path, last_part_only=False, part_name=None):

    if last_part_only == True:

        part_name = spark.sql(f"DESCRIBE DETAIL {f'{path}'}").selectExpr("partitionColumns").collect()[0][0][0]
        last_part = spark.read.table(path).selectExpr(f"max({part_name})").collect()[0][0]
        
        df = (
            spark.read.format("delta").load(path)
            .filter(F.col(part_name) == last_part)
        )

    else:
        df = (
            spark.read.format("delta").load(path)
        )
 
    return df


def test_spark():

    spark = (SparkSession.builder
    .appName("SparkTesting")
    .getOrCreate())

    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.show()
    print("OK: Spark working correctly.")

    

def drop_table(spark, path):
    spark.sql(f"drop table if exists {path}")
    print(f"TABLE_OPERATION: Table {path} sucessfuly dropped.")

def clear_table(spark, path):
    spark.sql(f"delete from {path}")
    print(f"TABLE_OPERATION: All lines from table {path} have been cleared successfully.")


def register_table(spark, name, path):
    try:
        spark.sql(f"CREATE TABLE {name} USING DELTA LOCATION'{path}'")
        print(f"TABLE_OPERATION: Table {name} in path {path} registered sucessfully.")
    except Exception as e:
        print(f"Error: {e}")

def create_database(spark, name, warehouse_location):
    try:
        if not spark.sql("SHOW DATABASES").filter(F.col("namespace") == name).isEmpty():
            print(f"DATABASE: Database {name} found.")
            spark.sql(f"DESCRIBE DATABASE EXTENDED {name};").show(truncate=False)
            spark.sql(f"SHOW TABLES IN {name}").show()
        else:
            print(f"DATABASE: Database {name} not found. Creating database in path {get_root_path()}/{warehouse_location}/spark-warehouse/{name}.db")
            spark.sql(f"create database if not exists {name}")
            print(f"DATABASE: Database {name} created sucessfully.")
    except Exception as e:
        print(f"Error: {e}")


def drop_database(spark, name):
    try:
            spark.sql(f"drop schema {name} cascade")
            print(f"DATABASE: Database {name} dropped sucessfully.")
    except Exception as e:
        print(f"Error: {e}")


def clear_partition(spark, path, target_partition:str = None, last_partition = False):

    if target_partition != None:
        last_partition = False

    if last_partition == True:
        target_partition = None

    try:
            part_name = spark.sql(f"DESCRIBE DETAIL {f'{path}'}").selectExpr("partitionColumns").collect()[0][0][0]

            if part_name:
                if last_partition and target_partition == None:
                    target_partition = spark.read.table(path).selectExpr(f"max({part_name})").collect()[0][0]

                spark.sql(f"delete from {path} where {part_name} = {target_partition}")
                print(f"TABLE_OPERATION: Partition {part_name} ({target_partition}) from table {path} cleared successfully.")
            else:
                print(f"TABLE_OPERATION: No data has been deleted, as no partition was found for table {path}.")
    except Exception as e:
        print(f"Error: {e}")
    

def save_table(spark, df, path:str, partition_column:str=None, mode="append", schema_option="merge"):
    '''
    Saves a df to specified path as a Delta table.

    Optional parameters:
    - partition_column = Column name to use as table partition. If not informed, the table won't be partitioned.
    - mode = 'append' (DEFAULT), 'overwrite' (Will delete all data from existing table where partitions are present both in table and in df.)
    - schema_option = 'merge' (DEFAULT), 'overwrite', 'none' (raises exception if schema mismatch)
    '''

    writer = df.write.format("delta").option("delta.appendOnly", "false")

    # Checking if table exists before proceeding
    if read_table(spark, path, return_empty_df_if_missing=True).isEmpty():
        print(f"TABLE_OPERATION: Table {path} does not exist yet. Changing to append mode to create table...")
        mode = "append"

    if mode=="append":
        writer = writer.mode(mode)

    elif mode=="overwrite":
        if partition_column:
            partitions_in_df = list(
                df.select(partition_column).distinct().toPandas()[partition_column]
            )

            try:
                df.createOrReplaceTempView("dataframe")
                for part in partitions_in_df:
                    clear_partition(spark, path, target_partition=part)
                    spark.sql(f"""
                        INSERT INTO {path}
                        SELECT * FROM dataframe WHERE {partition_column} = '{part}'
                    """)
                print(f"TABLE_OPERATION: Table {mode} saved successfully at {path}. Partitions overwritten: {partitions_in_df}")
            except Exception as e:
                print(f"TABLE_OPERATION: SAVE_TABLE ERROR - {e}")
                
            return
            
    else:
        raise Exception("Save table mode not supported. Please check supported modes and try again.")    


    if schema_option == "merge":
        writer = writer.option("mergeSchema", "true")
    elif schema_option == "overwrite":
        writer = writer.option("overwriteSchema", "true")


    if partition_column:
        writer = writer.partitionBy(partition_column)

    try:
        writer.saveAsTable(path)
        print(f"TABLE_OPERATION: Table {mode} saved successfully at {path}.")
    except Exception as e:
        print(f"TABLE_OPERATION: SAVE_TABLE ERROR - {e}")


def dataframe_to_csv(df, csv_name):
    csv_default_location = check_csv_path()
    csv_save_location = f"{csv_default_location}\\{csv_name}"
    df.limit(1048576).repartition(1).write.mode("overwrite").option("header", True).csv(csv_save_location)
    clear_unused_output_files("csv", csv_name)

    csv_curr_name = os.listdir(csv_save_location)[0]
    rename_file("csv", csv_save_location, csv_curr_name, csv_name)

    print(f"CSV: File saved successfully to path {csv_save_location}\\{csv_name}.csv.")


def dataframe_to_parquet(df, parquet_name):
    parquet_default_location = check_parquet_path()
    parquet_save_location = f"{parquet_default_location}\\{parquet_name}"
    df.repartition(1).write.mode("overwrite").parquet(parquet_save_location)
    clear_unused_output_files("parquet", parquet_name)

    parquet_curr_name = os.listdir(parquet_save_location)[0]
    rename_file("parquet", parquet_save_location, parquet_curr_name, parquet_name)

    print(f"PARQUET: File saved successfully to path {parquet_save_location}\\{parquet_name}.parquet.")


def check_csv_path():
    # Setting CSV Folder
    if not CSV_PATH:
        csv_location = f'{get_root_path()}\\excel'
    else:
        csv_location = CSV_PATH
    print(f"EXCEL: CSV folder location: {csv_location}")
    
    if not os.path.exists(csv_location):
            print(f"EXCEL: CSV location does not exists yet. Creating {csv_location} folder...")
            os.makedirs(csv_location)
    return csv_location

def check_parquet_path():
    # Setting PARQUET Folder
    if not PARQUET_PATH:
        parquet_location = f'{get_root_path()}\\parquet'
    else:
        parquet_location = PARQUET_PATH
    print(f"PARQUET: Parquet folder location: {parquet_location}")
    
    if not os.path.exists(parquet_location):
            print(f"PARQUET: Parquet location does not exists yet. Creating {parquet_location} folder...")
            os.makedirs(parquet_location)
    return parquet_location


def clear_unused_output_files(filetype:str, folder_name:str, path = None):
    supported_types = ["parquet", "csv"]
    deleted_files = []

    

    if filetype not in supported_types:
        print(f"WARNING: Clearing unused output files for the informed output type is not supported. Supported filetypes: {*supported_types,}")
    
    else:
        
        if not path:
            if filetype == "parquet":
                path = f"{get_root_path()}\\parquet\\{folder_name}"
            elif filetype == "csv":
                path = f"{get_root_path()}\\excel\\{folder_name}"

        files_in_dir = os.listdir(path)
        for f in files_in_dir:
            if f.endswith(".crc") or f.endswith("_SUCCESS"):
                os.remove(f"{path}\\{f}")
                deleted_files.append(f)
        print(f"FILE: Cleared {len(deleted_files)} unused files.")
    
    


def rename_file(filetype, path, target_file_name, new_name):

    supported_types = ["parquet", "csv"]

    if filetype not in supported_types:
        print(f"WARNING: Clearing unused output files for the informed output type is not supported. Supported filetypes: {*supported_types,}")
        status = False
    else:
        try:
            os.rename(f"{path}\\{target_file_name}", f"{path}\\{new_name}.{filetype}")
            status = True
        except FileNotFoundError:
            print("ERROR: File not found.")
            status = False
        except PermissionError:
            print("ERROR: Permission denied. Unable to rename the file.")
            status = False
    return status



def refresh_table_locations(spark, metastore_info:dict, location):
    
    location = f"{location}/spark-warehouse"

    db_name = metastore_info.get("DATABASE_NAME")
    tables = []

    for layer, layer_val in metastore_info.get("TABLES").items():
        for key, val in layer_val.items():
            tables.append(val)
    
    for table in tables:
        try:
            spark.sql(f"ALTER TABLE {db_name}.{table} SET LOCATION '{location}/{db_name}.db'")
            print(f"TB_LOCATION: Table {db_name}.{table} location updated to {location}/{db_name}.db .")
        except Exception as e:
            print(f"TB_LOCATION: {e}")