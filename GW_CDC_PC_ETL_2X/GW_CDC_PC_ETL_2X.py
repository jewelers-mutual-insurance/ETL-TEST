# ---------------------------------------------
# Importing standard libraries, AWS Glue modules, Spark utilities, 
# database connectors, geospatial tools, and logging support.
# ---------------------------------------------
import sys
from psycopg2 import sql
import psycopg2
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import botocore.exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.dynamicframe import DynamicFrame
from shapely import wkt
from pyspark.sql.functions import  udf, col, when, pandas_udf, PandasUDFType
from pyspark.sql.types import  BinaryType
from pyspark.sql import functions as F, Row, DataFrame
import logging
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import math
from pyspark import StorageLevel

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)

# Setup Spark and Glue Context
try:
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)  # Only one GlueContext here
    spark = glueContext.spark_session
    job = Job(glueContext)
    logger.info("Job started successfully")
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == 'AlreadyExistsException':
        logger.error("Session already exists. Reusing the existing session")
    else:
        logger.error(f"An error occurred : {str(e)}")
        sys.exit(1)
except Exception as e:
    logger.error(f"An unexpected error occurred : {str(e)}")
    sys.exit(1)

# Configuration variables for database access, secrets, and region
primary_key = "id"  
# for stage 
secret_name = "PreprodPersistent_PC"  
GWreadReplica_Creds = "GWreadReplica_Creds_PC_Stage"  
# for prod
# secret_name = "PostgresPersistent_PC"
# GWreadReplica_Creds = "GWreadReplica_PC"
region_name = "us-east-1"  

# to get credentials of the database from the secret manager
def get_secret(secret_name, region_name="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {str(e)}")
        raise

# Fetching database credentials securely from AWS Secrets Manager
secrets = get_secret(secret_name, region_name)
GWreadReplica_Creds = get_secret(GWreadReplica_Creds, region_name)

# Database connection configurations (default ones)
incremental_snapshot_db_options = {
    "url": GWreadReplica_Creds["snapshot"]["url"],
    "user": GWreadReplica_Creds["snapshot"]["user"],
    "password": GWreadReplica_Creds["snapshot"]["password"]
}

incremental_persistent_db_options = {
    "url": secrets["persistent"]["url"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

snapshot_db_options = {
    "url": GWreadReplica_Creds["snapshot"]["url"],
    "user": GWreadReplica_Creds["snapshot"]["user"],
    "password": GWreadReplica_Creds["snapshot"]["password"],
    "hashexpression":"id"
}

persistent_db_options = {
    "url": secrets["persistent"]["url"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"],
    "hashexpression":"id"
}

persistent_post_db_options = {
    "url": secrets["persistent"]["url_p"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

persistent_insert_deleted_record_db_options = {
    "url": secrets["persistent"]["url_d"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

persistent_staging_db_options = {
    "url": secrets["persistent"]["url_s"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

insert_db_options = {
    "url": secrets["persistent"]["url"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

snapshot_conn = {
    "url": GWreadReplica_Creds["snapshot"]["url"],
    "user": GWreadReplica_Creds["snapshot"]["user"],
    "password": GWreadReplica_Creds["snapshot"]["password"]
    }

static_conn = {
    "url": secrets["persistent"]["url"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"],
    "preactions": "SET ROLE insuranceplatform;" }

persistent_deleted_conn = {
    "url": secrets["persistent"]["url_d"],
    "user": secrets["persistent"]["user"],
    "password": secrets["persistent"]["password"]
}

# List of geo columns to be set to BinaryType
geo_col = ['losslocationspatialdenorm','spatialpoint', 'spatialpointdenorm']
max_retries = 3
retry_delay = 90  # seconds

# Function to convert WKT string to WKB (binary format)
@pandas_udf(BinaryType())
def wkt_to_wkb(wkt_series):
    return wkt_series.apply(lambda x: wkt.loads(x).wkb if x else None)

# This is for storing or processing geospatial data in binary format.
def convert_geo_columns(snapshot_diff_record, geo_col):
    try:
        for col in geo_col:
            if col in snapshot_diff_record.columns:
                snapshot_diff_record = snapshot_diff_record.withColumn(col, wkt_to_wkb(snapshot_diff_record[col]))
        return snapshot_diff_record
    except Exception as e:
        logger.error(f"Failed to convert the data into geography columns in the DataFrame : {str(e)}")
        raise

# Retrieves schema metadata (column name, data type, and character length)
def get_schema(conn, table_name):
    query = f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' and table_schema = 'public'"
    for attempt in range(1, max_retries + 1):
        try:
            return spark.read.format("jdbc") \
                .option("url", conn["url"]) \
                .option("query", query) \
                .option("user", conn["user"]) \
                .option("password", conn["password"]) \
                .load()
        except Exception as e:
            logger.error(f"Attempt {attempt}: Failed to retrieve column name, datatype and character_maximum_length from the {table_name} : {str(e)}")
            if attempt < max_retries:
                logger.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached for table {table_name}")
                raise

# Formats a data type string for SQL or metadata display.
# If the type is 'USER-DEFINED', it's converted to 'character varying'.
def format_dtype(dtype, length):
    if dtype == 'USER-DEFINED':
        dtype = 'character varying'
        length = None
    if length is not None:
        return f"{dtype}({length})"
    return f"{dtype}"

# ---------------------------------------------
# Compares source and target schemas to identify differences.
# Returns:
#   - Columns to drop (not present in source, excluding protected columns)
#   - Columns to add (new in source but missing in target)
#   - Data type mismatches between source and target
# ---------------------------------------------
def detect_schema_changes(snapshot_schema, static_schema):
    source_columns = {row["column_name"]: (row["data_type"], row["character_maximum_length"]) for row in snapshot_schema.collect()}
    target_columns = {row["column_name"]: (row["data_type"], row["character_maximum_length"]) for row in static_schema.collect()}

    # Define protected columns that should never be dropped
    protected_columns = {"_ctlastmodified", "_ctstatus", "_ctisdeleted", "_ctfirstinserted"}

    # Identify columns to drop (exist in target but not in source)
    drop_columns = set(target_columns.keys()) - set(source_columns.keys()) - protected_columns

    # Identify columns to add (exist in source but not in target)
    add_columns = {col: ('character varying' if dtype == 'USER-DEFINED' else dtype) for col, (dtype, character_maximum_length) in source_columns.items() if col not in target_columns}
    
    # Identify columns with data type changes
    datatype_changes = {col: source_columns[col] for col in source_columns if col in target_columns and (source_columns[col][0] != 'USER-DEFINED') and (source_columns[col] != target_columns[col] or source_columns[col][1] != target_columns[col][1]) }
    
    datatype_changes_col = {col: [format_dtype(*target_columns[col]), format_dtype(*source_columns[col])]  for col in source_columns if col in target_columns and (source_columns[col][0] != 'USER-DEFINED') and (source_columns[col] != target_columns[col] or source_columns[col][1] != target_columns[col][1]) }

    return {
        "drop_columns": drop_columns,
        "add_columns": add_columns,
        "datatype_changes": datatype_changes,
        "datatype_changes_col" : datatype_changes_col
    }

# ---------------------------------------------
# Applies schema changes to a target PostgreSQL table using psycopg2.
# Changes may include:
#   - Adding new columns
#   - Dropping NOT NULL constraints
#   - Modifying column data types
# The function commits each type of change separately and rolls back on failure.
# ---------------------------------------------
def apply_changes_to_static(db_config, changes, table_name):
    if not any(changes.values()):  
        # No schema changes detected. No action needed.
        return
    conn = None

    try:
        # Establish connection to AWS RDS
        conn = psycopg2.connect(
            dbname=db_config["url"].split('/')[-1],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["url"].split('/')[2].split(':')[0]
        )

        with conn.cursor() as cursor:
            sql_statements_add = []
            sql_statements_drop = []
            sql_statements_datachange = []

            # Generate ADD COLUMN statements
            for col, dtype in changes['add_columns'].items():
                check_column_exists_query = sql.SQL("""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = {} AND column_name = {}
                """).format(sql.Literal(table_name), sql.Literal(col))
                
                cursor.execute(check_column_exists_query)
                column_exists = cursor.fetchone()

                if not column_exists:  
                    sql_statements_add.append(sql.SQL("ADD COLUMN {} {}").format(
                        sql.Identifier(col), sql.SQL(dtype)
                    ))
                    
            if sql_statements_add:
                query = sql.SQL("ALTER TABLE {} {}").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql_statements_add)
                )
                logger.info(f"add query for {table_name}: {query}")
                cursor.execute(query)
                conn.commit()

            # Generate DROP COLUMN statements 
            for col in changes['drop_columns']:
                sql_statements_drop.append(sql.SQL("ALTER COLUMN {} DROP NOT NULL").format(sql.Identifier(col)))
                
            if sql_statements_drop:
                query = sql.SQL("ALTER TABLE {} {}").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql_statements_drop)
                )
                logger.info(f"drop not null for columns of {table_name}: {query}")
                cursor.execute(query)
                conn.commit()

            # Generate ALTER COLUMN TYPE statements
            for col, (new_dtype, character_maximum_length) in changes['datatype_changes'].items():
                if character_maximum_length is not None and isinstance(character_maximum_length, int):
                    sql_statements_datachange.append(sql.SQL("ALTER COLUMN {} SET DATA TYPE {}({})").format(
                        sql.Identifier(col), sql.SQL(new_dtype), sql.Literal(character_maximum_length)
                    ))
                else:
                    sql_statements_datachange.append(sql.SQL("ALTER COLUMN {} SET DATA TYPE {}").format(
                        sql.Identifier(col), sql.SQL(new_dtype)
                    ))

            # Execute ALTER TABLE in a single batch query
            if sql_statements_datachange:
                query = sql.SQL("ALTER TABLE {} {}").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql_statements_datachange)
                )

                logger.info(f"schema change query for {table_name} : {query}")
                cursor.execute(query)
                conn.commit()

    except Exception as e:
        if conn:  
            conn.rollback()
        logger.error(f"Error applying schema changes: {str(e)}")

    finally:
        if conn:  
            conn.close()  

# Function to process a table
def process_table(table_name, run_id, prev_run_start_time, curr_run_start_time, details_count):
    # ---------------------------------------------
    # Retrieves table schemas from source, target, and deleted databases.
    # Detects schema differences between source and target versions.
    # Applies schema changes to both target and deleted versions of the table.
    # ---------------------------------------------
    schema_changes = {}
    try:
        snapshot_schema = get_schema(snapshot_conn,table_name)
        static_schema = get_schema(static_conn,table_name)
        static_deleted_schema = get_schema(persistent_deleted_conn,table_name)
        schema_changes = detect_schema_changes(snapshot_schema, static_schema)
        deleted_schema_changes = detect_schema_changes(snapshot_schema, static_deleted_schema)
        apply_changes_to_static(static_conn,schema_changes,table_name)
        apply_changes_to_static(persistent_deleted_conn,deleted_schema_changes,table_name)
    
    except Exception as e:
        logger.error(f"Exception occurred when applying schema changes: {str(e)}")
        return

    # Prepare strings for reporting changes (drop/add/data type modifications)
    drop_columns = ', '.join(schema_changes.get('drop_columns', []))
    add_columns = ', '.join(schema_changes.get('add_columns', {}).keys())
    datatype_changes = ', '.join(f"{key}: {value[0]} to {value[1]}" for key, value in schema_changes.get('datatype_changes_col', {}).items())

    for key, value in schema_changes.items():
        if isinstance(value, list):
            value = ', '.join(map(str, value))
        else:
            value = str(value)
    # Get current Glue job metadata
    job_name, job_id = get_glue_job_name()
    scenario_info_df = None
    
    # ---------------------------------------------
    # Attempts to fetch scenario-specific metadata for the given table 
    # from the gw_cdc_metadata_config table in the target DB.
    # ---------------------------------------------
    fetch_scenario_query = f"""SELECT *	FROM public.gw_cdc_metadata_config where metatablename = '{table_name}' and metaschema = 'policycenter' """
    for attempt in range(1, max_retries + 1):
        try:
            scenario_info_df = spark.read.jdbc(url=secrets["persistent"]["url_p"],table=f"({fetch_scenario_query}) AS scenario",  properties=persistent_post_db_options)
            break
        except Exception as e:
            logger.error(f"Attempt {attempt}: Exception occurred while executing fetch_scenario_query: {str(e)}")
            if attempt < max_retries:
                logger.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached.")
                return

    # ---------------------------------------------
    # For each scenario row retrieved from metadata config:
    #   - If metacategory is 1 or 4, prepare incremental query
    #   - If metacategory is 2,3 or 5, full refresh
    # ---------------------------------------------
    for row in scenario_info_df.collect():
        try:
            if (str(row["metacategory"]) == "1") or (str(row["metacategory"]) == "4"): 
                snapshot_options = incremental_snapshot_db_options.copy()
                snapshot_options['dbtable'] = table_name.strip()
            
                persistent_options = incremental_persistent_db_options.copy()
                persistent_options['dbtable'] = table_name.strip()
                incremental_query = ''
                if str(row["metacategory"]) == "1":
                    incremental_query = f"""SELECT * FROM {table_name} WHERE {row["metaquery"]} between '{prev_run_start_time}' and '{curr_run_start_time}'"""
                else:
                    incremental_query = f"""SELECT main.* FROM {table_name} as main {row["metaquery"]} between '{prev_run_start_time}' and '{curr_run_start_time}'"""
                logger.info(f"incremental_query : {incremental_query}")
                                           
                snapshot_df = None
                source_count_df = None
                target_count_df = None
                count_query = f"""SELECT COUNT(*) as cnt, COALESCE(MIN(id), 0) as lowerbound, COALESCE(MAX(id), 0) as upperbound FROM {table_name}"""
                drop_query = f"DROP TABLE IF EXISTS updates_temp_{table_name}"
                # Drop temp table used for updates
                try:
                    update_table_from_query(drop_query, insert_db_options, table = f"updates_temp_{table_name}")
                except Exception as e:
                    logger.error(f"Exception occurred while executing drop_query for table updates_temp_{table_name}")
                    return
                
                # Fetch source/target row counts and ID bounds
                for attempt in range(1, max_retries + 1):
                    try:
                        source_count_df = spark.read.jdbc(url=GWreadReplica_Creds["snapshot"]["url"],table=f"({count_query}) AS snapshotct",  properties=snapshot_options)
                        target_count_df = spark.read.jdbc(url=secrets["persistent"]["url"],table=f"({count_query}) AS persistentcnt",  properties=persistent_options)
                        break
                    except Exception as e:
                        logger.error(f"Attempt {attempt}: Exception occurred while executing source and target row counts for table {table_name} : {str(e)}")
                        if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                        else:
                            logger.error("Max retries reached.")
                            return
                            
                # Execute incremental data pull from source
                for attempt in range(1, max_retries + 1): 
                    try:
                        snapshot_df = spark.read.jdbc(url=GWreadReplica_Creds["snapshot"]["url"],table=f"({incremental_query}) AS snapshot",  properties=snapshot_options)
                        snapshot_df.persist(StorageLevel.MEMORY_AND_DISK)
                        break  
                    except Exception as e:
                         log_db_name = GWreadReplica_Creds['snapshot']['url'].split('/')[3]
                         logger.error(f"Attempt {attempt}:  Exception occurred while executing incremental_query for table {table_name} in {log_db_name} database : {str(e)}")
                         if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                         else:
                            logger.error("Max retries reached.")
                            return
                
                # Get target table schema for column alignment
                target_columns = []
                try:
                    static_schema = get_schema(static_conn, table_name)
                    target_columns = [row["column_name"] for row in static_schema.collect()]
                except Exception as e:
                    logger.error(f"Exception occurred when applying schema changes: {str(e)}")
                    return

                # Select only columns common to both source snapshot and target schema,
                # extract ID bounds and calculate number of partitions,
                # then update JDBC options for partitioned reads.
                common_columns = list(set(snapshot_df.columns).intersection(set(target_columns)))
                snapshot_df = snapshot_df.select(common_columns)
                snapshot_full_id_df = None
                persistent_id_df = None
                row_val = source_count_df.collect()[0]
                lower_bound_val = row_val['lowerbound']
                upper_bound_val = row_val['upperbound']
                num_part_val = str(math.ceil(upper_bound_val/10000000))

                logger.info(f"lower_bound_val : {lower_bound_val} and upper_bound_val : {upper_bound_val} and num_partition : {num_part_val} for table : {table_name}")
                snapshot_options['partitionColumn'] = "id"
                snapshot_options['lowerBound'] = str(lower_bound_val)
                snapshot_options['upperBound'] = str(upper_bound_val)
                snapshot_options['numPartitions'] = str(num_part_val)
                persistent_options['partitionColumn'] = "id"
                persistent_options['lowerBound'] = str(lower_bound_val)
                persistent_options['upperBound'] = str(upper_bound_val)
                persistent_options['numPartitions'] = str(num_part_val)
                
                for attempt in range(1, max_retries + 1):
                    try:
                        snapshot_id_query = f"SELECT DISTINCT id FROM {table_name}"
                        snapshot_full_id_df = spark.read.jdbc(url=GWreadReplica_Creds["snapshot"]["url"],table=f"({snapshot_id_query}) AS snapshot_full_id_records", properties=snapshot_options)
                        snapshot_full_id_df = snapshot_full_id_df.persist(StorageLevel.MEMORY_AND_DISK)
                        break
                    except Exception as e:
                        log_db_name = GWreadReplica_Creds['snapshot']['url'].split('/')[3]
                        logger.error(f"Attempt {attempt}: Exception occurred while executing snapshot id query in {log_db_name} database : {str(e)}")
                        if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                        else:
                            logger.error("Max retries reached.")
                            return

                for attempt in range(1, max_retries + 1):
                    try:
                        persistent_id_query = f"SELECT DISTINCT id as id, _ctfirstinserted FROM {table_name}"
                        persistent_id_df = spark.read.jdbc(url=secrets["persistent"]["url"],table=f"({persistent_id_query}) AS persistent", properties=persistent_options)
                        break
                    except Exception as e:
                        log_db_name = secrets['persistent']['url'].split('/')[3]
                        logger.error(f"Attempt {attempt}: Exception occurred while executing persistent id query in {log_db_name} in database : {str(e)}")
                        if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                        else:
                            logger.error("Max retries reached.")
                            return

                # Cache the DataFrames to optimize repeated access
                snapshot_full_id_df = snapshot_full_id_df.cache()
                persistent_id_df = persistent_id_df.cache()
                # Identify new records in source that are not in target (to be inserted)
                insert_df = snapshot_df.join(persistent_id_df, "id", "left_anti")
                update_df = None
                # Identify records in source that need updates (excluding new inserts)
                if snapshot_df.count() > 0:
                    update_df = snapshot_df.join(insert_df, "id", "left_anti")
                else:
                    # If snapshot_df is empty, create an empty DataFrame with the same schema
                    fetch_update_records_query = f"SELECT * FROM {table_name} WHERE 1=0"
                    update_df = spark.read.jdbc(url=secrets["persistent"]["url"],table=f"({fetch_update_records_query}) AS fetch_del_query",  properties=persistent_options)
                # Identify records to delete: present in source but not in target
                delete_id_df = persistent_id_df.join(snapshot_full_id_df, "id", "left_anti")
                delete_id_list = [str(row["id"]) for row in delete_id_df.distinct().collect()]
                delete_id_str = ",".join(f"'{i}'" for i in delete_id_list)
                delete_df = None
                if delete_id_str:
                    # Fetch records that need to be deleted from target DB
                    fetch_delete_records_query = f"SELECT * FROM {table_name} WHERE id IN ({delete_id_str})"
                    for attempt in range(1, max_retries + 1):
                        try:
                            delete_df = spark.read.jdbc(url=secrets["persistent"]["url"],table=f"({fetch_delete_records_query}) AS fetch_del_query",  properties=persistent_options)
                            break
                        except Exception as e:
                            log_db_name = secrets['persistent']['url'].split('/')[3]
                            logger.error(f"Attempt {attempt}: Exception occurred while executing fetch_delete_records_query in {log_db_name} database : {str(e)}")
                            if attempt < max_retries:
                                logger.error(f"Retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                            else:
                                logger.error("Max retries reached.")
                                return
                else:
                    # No IDs to delete, create an empty DataFrame with the same schema
                    fetch_delete_records_query = f"SELECT * FROM {table_name} WHERE 1=0"
                    delete_df = spark.read.jdbc(url=secrets["persistent"]["url"],table=f"({fetch_delete_records_query}) AS fetch_del_query",  properties=persistent_options)

                # Initialize counts , calculate counts from DataFrames and extract counts from count DataFrames
                table_insert_count = ''; table_delete_count = ''; table_update_count = ''; source_count = ''; target_count = '';
                table_insert_count = insert_df.count()
                table_delete_count = delete_df.count()
                table_update_count = update_df.count()
                source_count = source_count_df.collect()[0]['cnt']
                target_count = target_count_df.collect()[0]['cnt']
                
                logger.info(f"snapshot_df : {source_count} and persistent_df : {target_count} for {table_name} ")
                logger.info(f"table_insert_count : {table_insert_count} and table_delete_count : {table_delete_count} and table_update_count : {table_update_count} for {table_name} before converting into dynamic dataframe")

                # Convert geography columns to binary (WKB format)
                insert_df = convert_geo_columns(insert_df, geo_col)
                delete_df = convert_geo_columns(delete_df, geo_col)
                update_df = convert_geo_columns(update_df, geo_col)

                logger.info(f"table_insert_count : {insert_df.count()} and table_delete_count : {delete_df.count()} and table_update_count : {update_df.count()} for {table_name} after convert_geo_columns")

                # Add/update control columns for inserts, deletes and update
                insert_df = insert_df.withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctfirstinserted", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('I'))
                delete_df = delete_df.withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('D'))
                update_df = update_df.alias("u").join(persistent_id_df.select("id", "_ctfirstinserted").alias("p"),on="id", how="left")
                update_df = update_df.withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('U'))

                logger.info(f"table_insert_count : {insert_df.count()} and table_delete_count : {delete_df.count()} and table_update_count : {update_df.count()} for {table_name} after 3 column additions")

                # Convert Spark DataFrames to Glue DynamicFrames for further processing
                insert_dyf = DynamicFrame.fromDF(insert_df, glueContext, "insert_dyf")
                delete_dyf = DynamicFrame.fromDF(delete_df, glueContext, "delete_dyf")
                update_dyf = DynamicFrame.fromDF(update_df, glueContext, "update_dyf")

                logger.info(f"table_insert_count : {insert_dyf.count()} and table_delete_count : {delete_dyf.count()} and table_update_count : {update_dyf.count()} for {table_name} after dynframe conv")

                # Prepare the update query with counts and schema change info
                if details_count is None:
                    update_query = f"update gw_cdc_meta_runtimes_pc_detail set runtableaddedcolumns = '{add_columns}' , runtabledroppedcolumns = '{drop_columns}', runtabledatatypechangescolumns = '{datatype_changes}',runtableinsertCount = {insert_dyf.count()},  runtabledeletecount = {delete_dyf.count()} , runtableupdateCount = {update_dyf.count()},runtablesourcecount = {source_count}, runtabletargetcount = {target_count} where runid = {run_id} and runtablename = '{table_name}'"

                    try:
                        update_table_from_query(update_query, persistent_post_db_options,  table = 'gw_cdc_meta_runtimes_pc_detail')
                    except Exception as e:
                        log_db_name = persistent_post_db_options['url'].split('/')[3]
                        logger.error(f"Error while inserting count record into gw_cdc_meta_runtimes_pc_detail for {table_name} in {log_db_name} database : {str(e)}")
                        return

                logger.info(f"table_insert_count : {insert_dyf.count()} and table_delete_count : {delete_dyf.count()} and table_update_count : {update_dyf.count()} for {table_name} after converting into dynamic dataframe")
                
                delete_df_id_list = delete_df.select('id')  #rlaksh
                update_df_id_list = update_df.select('id')  #rlaksh

                logger.info(f"update_df_id_list : {update_df_id_list.count()} for {table_name} ") #rlaksh0910
                duplicate_check = update_df_id_list.select('id').groupBy('id').count().filter("count > 1") #rlaksh0910
                logger.info(f"Number of duplicate IDs in update_df_id_list: {duplicate_check.count()} for {table_name} ") #rlaksh0910

                # First, handle archival/deletion to _deleted database
                if delete_dyf.count() > 0:
                    try:
                        #delete_records_from_rds(table_name, delete_df.select('id'), persistent_insert_deleted_record_db_options, 'direct_delete') -- rlaksh
                        delete_records_from_rds(table_name, delete_df_id_list, persistent_insert_deleted_record_db_options, 'direct_delete')
                        insert_deleted_record_to_rds(delete_dyf, table_name)
                    except Exception as e:
                        log_db_name = persistent_insert_deleted_record_db_options['url'].split('/')[3]
                        logger.error(f"Error while deleting {log_db_name} database record for {table_name}: {str(e)}")
                        return

                # Then, delete the same records from the main persistent table
                if delete_dyf.count() > 0:
                    try:
                        #delete_records_from_rds(table_name, delete_df.select('id'), incremental_persistent_db_options, 'direct_delete')  -- rlaksh
                        delete_records_from_rds(table_name, delete_df_id_list, incremental_persistent_db_options, 'direct_delete')
                    except Exception as e:
                        log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                        logger.error(f"Error while deleting {log_db_name} database record for {table_name}: {str(e)}")
                        return

                if update_dyf.count() > 0:
                    try:
                        staging_df = update_df.select('id', '_ctfirstinserted')
                        # Query to get all distinct IDs from the staging table
                        get_stage_id_query = f"select distinct id from {table_name}"
                        stage_id_df = None
                        
                         # Retry loop to fetch existing stage IDs with exponential backoff
                        for attempt in range(1, max_retries + 1):
                            try:
                                stage_id_df = spark.read.jdbc(url=secrets["persistent"]["url_s"],table=f"({get_stage_id_query}) AS get_stage_id_query",  properties=persistent_staging_db_options) 
                                break
                            except Exception as e:
                                log_db_name = secrets['persistent']['url_s'].split('/')[3]
                                logger.error(f"Attempt {attempt}: Error while retriving record from stage table {table_name} of {log_db_name} database : {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return
                        # Filter out IDs already present in staging from updates to avoid duplicates and insert new update records into the staging schema
                        filtered_stage_id_df = staging_df.join(stage_id_df, "id", "left_anti")
                        filtered_stage_id_dyf = DynamicFrame.fromDF(filtered_stage_id_df, glueContext, "filtered_stage_id_dyf")

                        logger.info(f"filtered_stage_id_df : {filtered_stage_id_df.count()} for {table_name} ") #rlaksh0910
                        duplicate_check = filtered_stage_id_df.select('id').groupBy('id').count().filter("count > 1") #rlaksh0910
                        logger.info(f"Number of duplicate IDs in filtered_stage_id_df: {duplicate_check.count()} for {table_name} ") #rlaksh0910
                        logger.info(f"filtered_stage_id_dyf : {filtered_stage_id_dyf.count()} for {table_name} ") #rlaksh0910
                        #duplicate_check = filtered_stage_id_dyf.select('id').groupBy('id').count().filter("count > 1") #rlaksh0910
                        #logger.info(f"Number of duplicate IDs in filtered_stage_id_dyf: {duplicate_check.count()} for {table_name} ") #rlaksh0910

                        insert_to_rds(filtered_stage_id_dyf, table_name, persistent_staging_db_options, func_name='insert', db_name = 'stage_schema')
                        
                        # Delete old records in persistent storage before applying new updates
                        #delete_records_from_rds(table_name, update_df.select('id'), incremental_persistent_db_options, 'update_delete') --rlaksh
                        logger.info(f"update_df_id_list : {update_df_id_list.count()} for {table_name} b4 callTodeleteRecordsFromRds")
                        delete_records_from_rds_for_update(table_name, update_df_id_list, incremental_persistent_db_options, 'update_delete')
                        max_attempts = 5
                        attempt = 1
                        remaining = -1
                        while attempt <= max_attempts:
                            #remaining = verify_deletion_loop(update_df.select('id'), incremental_persistent_db_options, table_name) --rlaksh
                            remaining = verify_deletion_loop(update_df_id_list, incremental_persistent_db_options, table_name)
                            logger.info(f"remaining value for {table_name} : {remaining}")
                            if remaining == 0:
                                logger.info(f"Verified: All target records were deleted for table {table_name}.")
                                time.sleep(10)
                                update_to_rds(update_dyf, table_name)
                                break
                            else:
                                logger.warning(f"Deletion is in progress for {table_name}")
                                #delete_records_from_rds(table_name, update_df.select('id'), incremental_persistent_db_options, 'update_delete') --rlaksh
                                delete_records_from_rds_for_update(table_name, update_df_id_list, incremental_persistent_db_options, 'update_delete')
                                time.sleep(60)
                                attempt += 1
                        if remaining != 0:
                            logger.error("Records not fully deleted after maximum retries.")
                            return
                        else:
                            log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                            logger.info(f"Successfully deleted {update_df.count()} records from table '{table_name}' of {log_db_name} database.")
                    except Exception as e:
                        log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                        logger.error(f"Error while updating record for {table_name} of of {log_db_name} database : {str(e)}")
                        return     

                # Insert new records into the main persistent database schema
                if insert_dyf.count() > 0:
                    try:
                        insert_to_rds(insert_dyf, table_name, insert_db_options, func_name='insert', db_name = 'main_schema')
                    except Exception as e:
                        log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                        logger.error(f"Error while inserting record for {table_name} of of {log_db_name} database: {str(e)}")
                        return

                # Query to get distinct IDs and their insertion timestamps from the staging table
                get_update_id_query = f"select distinct id, _ctfirstinserted from {table_name}"
                update_temp_table = f"updates_temp_{table_name}"

                # Proceed only if the table-level restart flag is not set ('N')
                try:
                    if details_count is not None:
                        update_id_df = None
                        for attempt in range(1, max_retries + 1):
                            try:
                                update_id_df = spark.read.jdbc(url=str(secrets["persistent"]["url_s"]),table=f"({get_update_id_query}) AS get_update_id_query",  properties=persistent_staging_db_options) 
                                break
                            except Exception as e:
                                log_db_name = secrets['persistent']['url_s'].split('/')[3]
                                logger.error(f"Attempt {attempt}: Error while retriving record from stage table {table_name} for updating of of {log_db_name} database : {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return

                        # Retry loop to overwrite/create a temporary update table in the database
                        for attempt in range(1, max_retries + 1):
                            try:
                                update_id_df.write.jdbc(
                                        url=insert_db_options["url"],
                                        table=f"{update_temp_table}",  
                                        mode="overwrite",  
                                        properties={
                                            "user": insert_db_options["user"],
                                            "password": insert_db_options["password"]
                                        }
                                    )
                                break
                            except Exception as e:
                                logger.error(f"Attempt {attempt}: Error while creating temp table {update_temp_table} : {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return

                        # Perform a bulk update on the target table using the temporary table and execute the bulk update query on the database
                        bulk_update_query = f"""
                        UPDATE {table_name} tgt
                        SET 
                            _ctstatus = 'U',
                            _ctfirstinserted = src._ctfirstinserted
                        FROM public.{update_temp_table} src
                        WHERE tgt.id = src.id
                        """
                        update_table_from_query(bulk_update_query, insert_db_options, table_name)
                except Exception as e:
                    logger.error(f"Error while retrieving update ID record from staging table {table_name} table: {str(e)}")
                    return

                # Truncating the staging table (CASCADE ensures dependencies are also removed) , updating `_ctstatus` to 'I' for records that were updated during this run and marking the table load as completed in the metadata tracking table
                truncate_query = f"TRUNCATE TABLE {table_name} CASCADE"
                update_ctstatus_query = f"update {table_name} set _ctstatus = 'I' where _ctstatus = 'U' and date(_ctfirstinserted) >= date('{curr_run_start_time}') "
                details_update_query = f"update gw_cdc_meta_runtimes_pc_detail set runendtime = '{datetime.now(ZoneInfo('America/Chicago'))}', RunJobId = '{job_id}',  runtableloadcompletedflag = 'Y' where runid = {run_id} and runtablename = '{table_name}'"
                logger.info(f"details_update_query : {details_update_query}")
                
                try:
                    if details_count is not None:
                        update_table_from_query(update_ctstatus_query, insert_db_options, table = table_name)
                        logger.info(f"update_ctstatus_query : {update_ctstatus_query}")
                        
                    # Truncate the staging table to clean up temporary data and  drop the temporary update table used during this run
                    update_table_from_query(details_update_query, persistent_post_db_options, table = 'gw_cdc_meta_runtimes_pc_detail')
                    update_table_from_query(truncate_query, persistent_staging_db_options, table_name)
                    
                    update_table_from_query(drop_query, insert_db_options, table = f"updates_temp_{table_name}")
                except Exception as e:
                    log_db_name = persistent_post_db_options['url'].split('/')[3]
                    logger.error(f"Error while inserting record into gw_cdc_meta_runtimes_pc_detail for table {table_name} of {log_db_name} database : {str(e)}")
                    return
                logger.info(f"The {table_name} table process has completed successfully.")
                
            else:

                # Create connection options for snapshot and persistent databases using table-specific config
                snapshot_options = snapshot_db_options.copy()
                snapshot_options['dbtable'] = table_name.strip()
                # snapshot_options['hashpartitions'] = row["metahashpartitions"]

                persistent_options = persistent_db_options.copy()
                persistent_options['dbtable'] = table_name.strip()
                # persistent_options['hashpartitions'] = row["metahashpartitions"]

                # Initialize DynamicFrames to None
                snapshot_dyf = None
                persistent_dyf = None
                
                # Drop any previously created temporary update table before starting new processing
                drop_query = f"DROP TABLE IF EXISTS updates_temp_{table_name}"
                try:
                    update_table_from_query(drop_query, insert_db_options, table = f"updates_temp_{table_name}")
                except Exception as e:
                    logger.error(f"Exception occurred while executing drop_query for table updates_temp_{table_name} : {str(e)}")
                    return

                # Load source data from the source  into a DynamicFrame
                for attempt in range(1, max_retries + 1):
                    try:
                        snapshot_dyf = glueContext.create_dynamic_frame.from_options(connection_type="postgresql", connection_options=snapshot_options)
                        break
                    except Exception as e:
                         log_db_name = snapshot_options['url'].split('/')[3]
                         logger.error(f"Attempt {attempt}: Error while connecting to snapshot database for {table_name} of {log_db_name} database : {str(e)}")
                         if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                         else:
                            logger.error("Max retries reached.")
                            return

                # Load target into a DynamicFrame for comparison or transformation
                for attempt in range(1, max_retries + 1):
                    try:
                        persistent_dyf = glueContext.create_dynamic_frame.from_options(connection_type="postgresql", connection_options=persistent_options)
                        break
                    except Exception as e:
                         log_db_name = persistent_options['url'].split('/')[3]
                         logger.error(f"Attempt {attempt}: Error while connecting to persistent database for {table_name} of {log_db_name} database : {str(e)}")
                         if attempt < max_retries:
                            logger.error(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                         else:
                            logger.error("Max retries reached.")
                            return

                logger.info(f"snapshot_df : {snapshot_dyf.count()} and persistent_dyf : {persistent_dyf.count()} for {table_name} ")
                
                # Convert Glue DynamicFrames to Spark DataFrames and cache them for performance and extract '_ctfirstinserted' column for future reference during updates
                snapshot_df = snapshot_dyf.toDF().cache()
                persistent_df = persistent_dyf.toDF().cache()
                persistent_ctfirst_df = persistent_df.select(['id', '_ctfirstinserted'])

                # Count records from source and target for metrics 
                source_count = snapshot_dyf.count()
                target_count = persistent_dyf.count()

                # Identify common columns between source and target for accurate comparison and define specific columns to consider when comparing for updates (optional filtering)
                common_columns = list(set(snapshot_df.columns).intersection(set(persistent_df.columns)))
                update_comp_columns = ['id', 'typecode', 'description']
                valid_update_comp_columns = [col for col in update_comp_columns if col in common_columns]
                
                # Select only common columns from both DataFrames for aligned comparison
                snapshot_df = snapshot_df.select(common_columns)
                persistent_df = persistent_df.select(common_columns)

                # Add MD5 hash column based on concatenated common column values to detect row-level changes
                snapshot_df = snapshot_df.withColumn("hash", F.md5(F.concat_ws("||", *common_columns)))
                persistent_df = persistent_df.withColumn("hash", F.md5(F.concat_ws("||", *common_columns)))

                # Identify new records: Present in source but not in target ,deleted records: Present in target but not in source and updated records: Same ID exists in both, but the hash values differ
                insert_df = snapshot_df.join(persistent_df, "id", "left_anti")
                delete_df = persistent_df.join(snapshot_df, "id", "left_anti")
                update_df = snapshot_df.alias('snapshot').join(persistent_df.alias('persistent'), "id").filter(snapshot_df["hash"] != persistent_df["hash"])
                table_insert_count = ''; table_delete_count = ''; table_update_count = ''

                # Conditional logic to handle custom update logic if the table is flagged
                if row['metatabletypeflag'] == 'Y':
                    # Recalculate hash using only the validated comparison columns for update detection
                    snapshot_df_temp = snapshot_df.withColumn("hash", F.md5(F.concat_ws("||", *valid_update_comp_columns)))
                    persistent_df_temp = persistent_df.withColumn("hash", F.md5(F.concat_ws("||", *valid_update_comp_columns)))
                    
                    # Identify updated records by comparing the hashes using the selected columns
                    update_df_temp = snapshot_df_temp.alias('snapshot_temp').join(persistent_df_temp.alias('persistent_temp'), "id").filter(snapshot_df_temp["hash"] != persistent_df_temp["hash"]).select("id").withColumnRenamed("id", "temp_id")
                    table_update_count = update_df_temp.count()
                    
                    # Join and add _ctstatus and _ctlastmodified
                    update_df = update_df.join(update_df_temp, update_df["id"] == update_df_temp["temp_id"], "left") \
                                         .withColumn("_ctstatus", when(col("temp_id").isNotNull(), F.lit("U"))) \
                                         .withColumn("_ctlastmodified", when(col("temp_id").isNotNull(), F.from_utc_timestamp(F.current_timestamp(), "America/Chicago"))) \
                                         .drop("temp_id")
                else:
                    # Default update count using full comparison if special flag is not set
                    table_update_count = update_df.count()

                # Count the insert and delete records
                table_insert_count = insert_df.count()
                table_delete_count = delete_df.count()
                logger.info(f"table_insert_count : {table_insert_count} and table_delete_count : {table_delete_count} and table_update_count : {table_update_count} for {table_name}")

                # If no existing detail record, update metadata tracking table with counts and schema change info
                if details_count is None:
                    update_query = f"update gw_cdc_meta_runtimes_pc_detail set runtableaddedcolumns = '{add_columns}' , runtabledroppedcolumns = '{drop_columns}', runtabledatatypechangescolumns = '{datatype_changes}',runtableinsertCount = {table_insert_count},  runtabledeletecount = {table_delete_count} , runtableupdateCount = {table_update_count}, runtablesourcecount = {source_count}, runtabletargetcount = {target_count} where runid = {run_id} and runtablename = '{table_name}'"

                    # Execute the update query to log the metrics
                    try:
                        update_table_from_query(update_query, persistent_post_db_options, table = 'gw_cdc_meta_runtimes_pc_detail')
                    except Exception as e:
                        log_db_name = persistent_post_db_options['url'].split('/')[3]
                        logger.error(f"Error while inserting count record into gw_cdc_meta_runtimes_pc_detail for table {table_name} of {log_db_name} database : {str(e)}")
                        return

                delete_df_id_list = delete_df.select('id')

                # If there are any records to delete
                if delete_df.count() > 0:
                    try:
                        # Drop the 'hash' column , add metadata columns to track deletion status and timestamp and  delete those records from the _delete schema/database
                        logger.info(f"delete_df : {delete_df.count()} before adding _ctfirstinserted")
                        delete_df = delete_df.alias("u").join(persistent_ctfirst_df.alias("p"), on="id", how="left").select("u.*", F.col("p._ctfirstinserted"))
                        logger.info(f"delete_df : {delete_df.count()} after adding _ctfirstinserted")
                        delete_df = delete_df.drop("hash").withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('D'))
                        delete_df = convert_geo_columns(delete_df, geo_col)
                        logger.info(f"delete_df : {delete_df.count()} after geo convertion")
                        #delete_records_from_rds(table_name, delete_df.select('id'), persistent_insert_deleted_record_db_options, 'direct_delete') --rlaksh
                        delete_records_from_rds(table_name, delete_df_id_list, persistent_insert_deleted_record_db_options, 'direct_delete')
                        delete_dyf = DynamicFrame.fromDF(delete_df, glueContext, "delete_dyf")
                        insert_deleted_record_to_rds(delete_dyf, table_name)
                    except Exception as e:
                        log_db_name = persistent_insert_deleted_record_db_options['url'].split('/')[3]
                        logger.error(f"Error while deleting {log_db_name} database  record for {table_name}: {str(e)}")
                        return

                # Now delete the same records from the persistent main table
                if delete_df.count() > 0:
                    try:
                        logger.info(f"delete_df : {delete_df.count()} before deleting records from main database")
                        #delete_records_from_rds(table_name, delete_df.select('id'), incremental_persistent_db_options, 'direct_delete') --rlaksh
                        delete_records_from_rds(table_name, delete_df_id_list, incremental_persistent_db_options, 'direct_delete')
                    except Exception as e:
                        log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                        logger.error(f"Error while deleting record for table {table_name} of {log_db_name} database : {str(e)}")
                        return

                # Process update records
                if update_df.count() > 0:
                    try:
                        update_df = update_df.drop("hash")
                        update_df = convert_geo_columns(update_df, geo_col)
                        if row['metatabletypeflag'] == 'Y':
                            selected_columns = [f"snapshot.{col}" for col in common_columns] + ["_ctstatus", "_ctlastmodified"]
                            update_df = update_df.select(*selected_columns)
                        else:
                            selected_columns = [f"snapshot.{col}" for col in common_columns]
                            update_df = update_df.select(*selected_columns)
                            update_df = update_df.withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('U'))
                        joined_df = update_df.alias("u").join(persistent_ctfirst_df.alias("p"),on="id", how="left")
                        update_df = joined_df.select("u.*", F.col("p._ctfirstinserted"))
                        staging_df = update_df.select('id', '_ctfirstinserted')
                        get_stage_id_query = f"select distinct id from {table_name}"
                        stage_id_df = None
                        update_df_id_list = update_df.select('id')
                        for attempt in range(1, max_retries + 1):
                            try:
                                stage_id_df = spark.read.jdbc(url=str(secrets["persistent"]["url_s"]),table=f"({get_stage_id_query}) AS get_stage_id_query",  properties=persistent_staging_db_options) 
                                break
                            except Exception as e:
                                logger.error(f"Attempt {attempt}: Error while retriving record from stage table {table_name}: {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return
                        filtered_stage_id_df = staging_df.join(stage_id_df, "id", "left_anti")
                        filtered_stage_id_dyf = DynamicFrame.fromDF(filtered_stage_id_df, glueContext, "filtered_stage_id_dyf")
                        insert_to_rds(filtered_stage_id_dyf, table_name, persistent_staging_db_options, func_name='insert', db_name = 'stage_schema')
                        #delete_records_from_rds(table_name, update_df.select('id'), incremental_persistent_db_options, 'update_delete') --rlaksh
                        delete_records_from_rds_for_update(table_name, update_df_id_list, incremental_persistent_db_options, 'update_delete')
                        max_attempts = 5
                        attempt = 1
                        remaining = -1
                        while attempt <= max_attempts:
                            #remaining = verify_deletion_loop(update_df.select('id'), incremental_persistent_db_options, table_name) -- rlaksh
                            remaining = verify_deletion_loop(update_df_id_list, incremental_persistent_db_options, table_name)
                            logger.info(f"remaining value for {table_name} : {remaining}")
                            if remaining == 0:
                                logger.info(f"Verified: All target records were deleted for table {table_name}.")
                                time.sleep(10)
                                update_dyf = DynamicFrame.fromDF(update_df, glueContext, "update_dyf")
                                logger.info(f"fullrefresh section update_dyf creation : {update_dyf.count()} for {table_name} prior to update-insert")
                                update_to_rds(update_dyf, table_name)
                                break
                            else:
                                logger.warning(f"Deletion is in progress for {table_name}. {remaining} records is in progress delete")
                                #delete_records_from_rds(table_name, update_df.select('id'), incremental_persistent_db_options, 'update_delete') -- rlaksh
                                delete_records_from_rds_for_update(table_name, update_df_id_list, incremental_persistent_db_options, 'update_delete')
                                time.sleep(60)
                                attempt += 1
                        if remaining != 0:
                            logger.error("Records not fully deleted after maximum retries.")
                            return
                        else:
                            log_db_name = incremental_persistent_db_options['url'].split('/')[3]
                            logger.info(f"Successfully deleted {update_df.count()} records from table '{table_name}' of {log_db_name} database.")
                    except Exception as e:
                        logger.error(f"Error while updating record for {table_name} of main database: {str(e)}")
                        return      

                # Process insert records
                if insert_df.count() > 0:
                    try:
                        # Drop the 'hash' column and dd metadata columns (_ctlastmodified, _ctfirstinserted, _ctstatus)
                        insert_df = insert_df.drop("hash").withColumn("_ctlastmodified", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctfirstinserted", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago")).withColumn("_ctstatus", F.lit('I'))
                        insert_df = convert_geo_columns(insert_df, geo_col)
                        insert_dyf = DynamicFrame.fromDF(insert_df, glueContext, "insert_dyf")
                        insert_to_rds(insert_dyf, table_name, insert_db_options, func_name='insert', db_name = 'main_schema')
                    except Exception as e:
                        log_db_name = insert_db_options['url'].split('/')[3]
                        logger.error(f"Error while inserting record for {table_name} of {log_db_name} database : {str(e)}")
                        return

                get_update_id_query = f"select id, _ctfirstinserted from {table_name}"
                update_temp_table = f"updates_temp_{table_name}"
                
                # Proceed only if the table-level restart flag is not set ('N')
                try:
                    if details_count is not None:
                        update_id_df = None
                        for attempt in range(1, max_retries + 1):
                            try:
                                update_id_df = spark.read.jdbc(url=str(secrets["persistent"]["url_s"]),table=f"({get_update_id_query}) AS get_update_id_query",  properties=persistent_staging_db_options)
                                break
                            except Exception as e:
                                log_db_name = secrets['persistent']['url_s'].split('/')[3]
                                logger.error(f"Attempt {attempt}: Error while retriving record from stage table {table_name} for updating of {log_db_name} database : {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return

                        # Retry loop to overwrite/create a temporary update table in the database
                        for attempt in range(1, max_retries + 1):
                            try:
                                update_id_df.write.jdbc(
                                    url=insert_db_options["url"],
                                    table= f"{update_temp_table}", 
                                    mode="overwrite",  
                                    properties={
                                        "user": insert_db_options["user"],
                                        "password": insert_db_options["password"]
                                    }
                                )
                                break
                            except Exception as e:
                                logger.error(f"Attempt {attempt}: Error while creating temp table {update_temp_table} : {str(e)}")
                                if attempt < max_retries:
                                    logger.error(f"Retrying in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                else:
                                    logger.error("Max retries reached.")
                                    return
                                
                        # Perform a bulk update on the target table using the temporary table and execute the bulk update query on the database
                        bulk_update_query = f"""
                            UPDATE {table_name} tgt
                            SET 
                                _ctstatus = 'U',
                                _ctfirstinserted = src._ctfirstinserted
                            FROM {update_temp_table} src
                            WHERE tgt.id = src.id"""
                        update_table_from_query(bulk_update_query, insert_db_options, table_name)
                except Exception as e:
                    log_db_name = insert_db_options['url'].split('/')[3]
                    logger.error(f"Error while retrieving update id record from staging table {table_name} of {log_db_name} database : {str(e)}")
                    return

                # Truncating the staging table (CASCADE ensures dependencies are also removed) , updating `_ctstatus` to 'I' for records that were updated during this run and marking the table load as completed in the metadata tracking table
                truncate_query = f"TRUNCATE TABLE {table_name} CASCADE"
                update_ctstatus_query = f"update {table_name} set _ctstatus = 'I' where _ctstatus = 'U' and date(_ctfirstinserted) >= date('{curr_run_start_time}') "
                details_update_query = f"update gw_cdc_meta_runtimes_pc_detail set runendtime = '{datetime.now(ZoneInfo('America/Chicago'))}', RunJobId = '{job_id}', runtableloadcompletedflag = 'Y' where runid = {run_id} and runtablename = '{table_name}'"
                logger.info(f"details_update_query : {details_update_query}")
                
                try:
                    if details_count is not None:
                        update_table_from_query(update_ctstatus_query, insert_db_options, table = table_name)
                        logger.info(f"update_ctstatus_query : {update_ctstatus_query}")
                    
                    # Truncate the staging table to clean up temporary data and  drop the temporary update table used during this run
                    update_table_from_query(details_update_query, persistent_post_db_options, table = 'gw_cdc_meta_runtimes_pc_detail')
                    update_table_from_query(truncate_query, persistent_staging_db_options, table_name)
                    update_table_from_query(drop_query, insert_db_options, table = f"updates_temp_{table_name}")
                except Exception as e:
                    log_db_name = persistent_staging_db_options['url'].split('/')[3]
                    logger.error(f"Error while inserting records into gw_cdc_meta_runtimes_pc_detail for table {table_name} of {log_db_name} database : {str(e)}")
                    return
                logger.info(f"The {table_name} table process has completed successfully.")

        except Exception as e:
            logger.error(f"Error occurred for {table_name}: {str(e)}")
            continue

# Function to calculate number of hash partitions based on record count and assumes approximately 5 million records per partition
def calculate_hash_partitions(dynamic_frame):
    tot_rec = dynamic_frame.count()
    return str(math.ceil(tot_rec/7000000))

# General-purpose function to insert records from a DynamicFrame into a PostgreSQL RDS table
def insert_to_rds(dynamic_frame, table_name, db_options, func_name, db_name):
    hash_partition_num = calculate_hash_partitions(dynamic_frame)
    logger.info(f"hashpartitions for {func_name} - {db_name} - {table_name} : {hash_partition_num}")
    db_options['dbtable'] = table_name
    db_options['hashfield'] = "id"
    db_options['hashpartitions'] = str(hash_partition_num)   

    for attempt in range(1, max_retries + 1):
        try:
            glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="postgresql",
                connection_options=db_options
            )
            logger.info(f"record {func_name}ed successfully for {db_name} table {table_name} : {dynamic_frame.count()}")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error while {func_name}ing records for {db_name} table {table_name}: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached. Failed to {func_name} records for {table_name}.")
                raise

# Function to insert deleted records into a dedicated deleted-records RDS table
def insert_deleted_record_to_rds(dynamic_frame, table_name):
    persistent_insert_deleted_record_db_options['dbtable'] = table_name  # Set the table name dynamically for each table
    for attempt in range(1, max_retries + 1):
        try:
            glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="postgresql",
                connection_options=persistent_insert_deleted_record_db_options
            )
            logger.info(f"Deleted records inserted successfully for {table_name} : {dynamic_frame.count()}")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error while inserting deleted records to the table  {table_name} : {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached. Failed to insert deleted records for {table_name}.")
                raise


# Function to delete records from RDS PostgreSQL table using a temporary staging table
def delete_records_from_rds(table_name, unmatched_df, db_options, action):
    logger.info(f"unmatched_df : {unmatched_df.count()} for {table_name} inside delete_records_from_rds")
    
    for attempt in range(1, max_retries + 1):
        try:
            # Write unmatched IDs to a temporary table for use in delete SQL
            unmatched_df.write.jdbc(
                    url=db_options["url"],
                    table=f"temp_unmatched_ids_{table_name}", 
                    mode="overwrite",  
                    properties={
                        "user": db_options["user"],
                        "password": db_options["password"]
                    }
                )
            break
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error while creating temp table of deleting records for {table_name} : {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached for {table_name}.")
                raise

    # Prepare SQL to delete from main table using temporary table as filter
    delete_sql = f"""
            DELETE FROM public.{table_name}
            USING temp_unmatched_ids_{table_name}
            WHERE public.{table_name}.{primary_key} =  temp_unmatched_ids_{table_name}.{primary_key}
        """
    logger.info(f"delete_sql : {delete_sql}")
    conn = psycopg2.connect(
            host=db_options["url"].split("://")[1].split(":")[0],  # Extract host from URL
            port=db_options["url"].split(':')[3].split('/')[0],  # Extract port from URL
            dbname=db_options["url"].split("/")[3],  # Extract database name from URL
            user=db_options["user"],
            password=db_options["password"]
        )
    cur = conn.cursor()

    try:
        cur.execute(delete_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
        
        
# Function to delete records from RDS PostgreSQL table using a staging_databse
def delete_records_from_rds_for_update(table_name, unmatched_df, db_options, action):
    logger.info(f"unmatched_df : {unmatched_df.count()} for {table_name} inside delete_records_from_rds_for_update")

    # Prepare SQL to delete from main table using temporary table as filter
    delete_sql = f"""
            DELETE FROM public.{table_name}
            USING pc_staging_fdw.{table_name}
            WHERE public.{table_name}.{primary_key} =  pc_staging_fdw.{table_name}.{primary_key}
        """
    logger.info(f"delete_sql : {delete_sql}")
    conn = psycopg2.connect(
            host=db_options["url"].split("://")[1].split(":")[0],  # Extract host from URL
            port=db_options["url"].split(':')[3].split('/')[0],  # Extract port from URL
            dbname=db_options["url"].split("/")[3],  # Extract database name from URL
            user=db_options["user"],
            password=db_options["password"]
        )
    cur = conn.cursor()

    try:
        cur.execute(delete_sql)
        conn.commit()
                     
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
        

def verify_deletion_loop(unmatched_df, db_options, table_name):
    check_conn = psycopg2.connect(
            host=db_options["url"].split("://")[1].split(":")[0],  # Extract host from URL
            port=db_options["url"].split(':')[3].split('/')[0],  # Extract port from URL
            dbname=db_options["url"].split("/")[3],  # Extract database name from URL
            user=db_options["user"],
            password=db_options["password"]
        )
    check_cur = check_conn.cursor()
    verify_sql = f"""
        SELECT COUNT(main.id) FROM {table_name} AS main
        JOIN temp_unmatched_ids_{table_name} temp ON main.id = temp.id
    """
    check_cur.execute(verify_sql)
    check_conn.commit()
    remaining = check_cur.fetchone()[0]
    check_cur.close()
    check_conn.close()
    return remaining

# Function to update data in the main schema by calling insert_to_rds with 'update' flag
def update_to_rds(unmatched_df, table_name):
    try:
        insert_to_rds(unmatched_df, table_name, insert_db_options, func_name = 'update', db_name = 'main_schema')
    except Exception as e:
        logger.error(f"Error updating records for table {table_name}: {str(e)}") 
        raise

# Utility function to fetch the AWS Glue job name and run ID
def get_glue_job_name():
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        return args['JOB_NAME'], args['JOB_RUN_ID']
    except Exception as e:
        logger.error(f"Error retrieving Glue job name: {str(e)}")
        return "NA", "NA"

# Function to fetch or initialize the latest meta run information for a Glue job
def fetch_latest_metarun_info(meta_runtime_query):
    logger.info(f"meta_runtime_query : {meta_runtime_query}")
    idstarttime_lst_df = None
    
    # Attempt to fetch metadata from PostgreSQL using a retry mechanism
    for attempt in range(1, max_retries + 1):
        try:
            idstarttime_lst_df = spark.read.jdbc(url=secrets["persistent"]["url_p"],table=f"({meta_runtime_query}) AS meta_runtime",  properties=persistent_post_db_options)
            break
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error while executing meta_runtime_query : {str(e)}")
            if attempt < max_retries:
                logger.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached.")
                raise

    # Initialize default and return variables
    default_run_id = 100000
    run_id = ''
    curr_run_start_time = ''
    prev_run_start_time = ''
    restart_flag = ''
    job_name, job_id = get_glue_job_name()

    # If no previous run data found
    if idstarttime_lst_df.rdd.isEmpty():
        run_id = default_run_id
        curr_run_start_time = datetime.now(ZoneInfo("America/Chicago")) 
        prev_run_start_time = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Create initial metadata row
        initialize_meta_runtimes_detail_dict = {'RunId' : run_id, 
                                                     'RunJobName' : job_name,
                                                     'RunJobId' : job_id,
                                                    #  'RunStartTime' : datetime.now(ZoneInfo("America/Chicago")).strftime('%Y-%m-%d %H:%M:%S'),
                                                     'RunCompletedFlag' : "N"
                        }
        data = [Row(**initialize_meta_runtimes_detail_dict)]
        meta_df = spark.createDataFrame(data)
        meta_df = meta_df.withColumn("RunStartTime", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago"))

        # Write initial metadata to the control table
        try:
            meta_df.write.jdbc(url=secrets["persistent"]["url_p"], table='gw_cdc_meta_runtimes_pc', mode="append", properties = persistent_post_db_options)
            logger.info(f"meta_runtimes_detail data initialized successfully into gw_cdc_meta_runtimes_pc ")
        except Exception as e:
            logger.error(f"Error while initializing records into the gw_cdc_meta_runtimes_pc  : {e}")
            raise
        restart_flag = 'N'
        
    else:
        # Retrieve the latest run info
        max_row = idstarttime_lst_df.orderBy(col("runid").desc()).limit(1).collect()[0]
        run_completed_flag = max_row["runcompletedflag"]
        
        # If the last run completed successfully, initialize a new run
        if run_completed_flag == 'Y':
            run_id = int(max_row["runid"]) + 1
            prev_run_start_time = max_row["runstarttime"]
            # RunStartTime = datetime.now(ZoneInfo("America/Chicago"))
            initialize_meta_runtimes_detail_dict = {'RunId' : run_id, 
                                                     'RunJobName' : job_name,
                                                     'RunJobId' : job_id,
                                                    #  'RunStartTime' : datetime.now(ZoneInfo("America/Chicago")).strftime('%Y-%m-%d %H:%M:%S'),
                                                     'RunCompletedFlag' : "N"
                        }
            curr_run_start_time = datetime.now(ZoneInfo("America/Chicago"))
            data = [Row(**initialize_meta_runtimes_detail_dict)]
            meta_df = spark.createDataFrame(data)
            meta_df = meta_df.withColumn("RunStartTime", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago"))

            try:
                meta_df.write.jdbc(url=secrets["persistent"]["url_p"], table='gw_cdc_meta_runtimes_pc', mode="append", properties = persistent_post_db_options)
                logger.info(f"meta_runtimes_detail data initialized successfully for gw_cdc_meta_runtimes_pc")
            except Exception as e:
                logger.error(f"Error while initializing records into the gw_cdc_meta_runtimes_pc for gw_cdc_meta_runtimes_pc : {e}")
                raise
            restart_flag = 'N'
            
         # Else, if previous run did not complete, flag for restart
        else:
            restart_flag = 'Y'
            run_id = max_row["runid"]
            curr_run_start_time = max_row["runstarttime"]
            
            # Try to fetch the start time of the previous run
            previous_run = idstarttime_lst_df.filter(col("runid") == run_id - 1).collect()
            
            if previous_run:
                prev_run_start_time = previous_run[0]["runstarttime"]
            else:
                prev_run_start_time = max_row["runstarttime"]
        logger.info(f"utc_time : {datetime.now()} , curr_run_start_time : {curr_run_start_time} and prev_run_start_time : {prev_run_start_time}")
        
    return run_id, prev_run_start_time, curr_run_start_time, restart_flag


# Function to execute an arbitrary SQL query (usually an UPDATE or DDL) with retry
def update_table_from_query(query, db_option , table):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=db_option["url"].split("://")[1].split(":")[0],  # Extract host from URL
                port=db_option["url"].split(':')[3].split('/')[0],  # Extract port from URL
                dbname=db_option["url"].split("/")[3],  # Extract database name from URL
                user=db_option["user"],
                password=db_option["password"]
            )
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error connecting/updating records of the {table} : {str(e)}")
            if attempt < max_retries:
                logger.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached.")
                raise

# Running the process in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=65) as executor:
    time.sleep(600)
    # Query to fetch run ID, start time, and completion flag from the runtime metadata table
    meta_runtime_query = f"select runid, runstarttime, runcompletedflag from public.gw_cdc_meta_runtimes_pc"
    run_id , prev_run_start_time, curr_run_start_time, restart_flag = fetch_latest_metarun_info(meta_runtime_query)
    
    # Fetch Glue job name and ID and extract source and target DB connection URLs
    job_name, job_id = get_glue_job_name()
    source_url = GWreadReplica_Creds["snapshot"]["url"]
    taregt_url = secrets["persistent"]["url"]
    logger.info(f"source_url : {source_url}, taregt_url : {taregt_url},  run_id : {run_id}, prev_run_start_time : {prev_run_start_time} , curr_run_start_time : {curr_run_start_time}, restart_flag : {restart_flag}, job_id : {job_id}")
    table_lst_query = ''
    
    if restart_flag == "N":
        # Query to get list of tables from metadata config (initial run setup)
        table_lst_query = f"""SELECT metatablename, null as runtableinsertcount FROM gw_cdc_metadata_config where metaschema = 'policycenter' and  meta_worker = 2"""
        logger.info(f"table_lst_query : {table_lst_query}")
        
        # Fetch full metadata config details including hashpartition and type flags
        fetch_metadataconfig_query = f"""SELECT * FROM public.gw_cdc_metadata_config where metaschema = 'policycenter'"""
        metadataconfig_info_df = None
        
        # Retry logic to handle transient DB read issues while fetching config
        for attempt in range(1, max_retries + 1):
            try:
                metadataconfig_info_df = spark.read.jdbc(url=secrets["persistent"]["url_p"],table=f"({fetch_metadataconfig_query}) AS scenario",  properties=persistent_post_db_options)
                break
            except Exception as e:
                logger.error(f"Attempt {attempt}: Error while retriving deatails from the gw_cdc_metadata_config table: {str(e)}")
                if attempt < max_retries:
                    logger.error(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached.")
                    raise

        # Build per-table metadata entries for runtime detail table
        meta_data = []
        # RunStartTime = datetime.now(ZoneInfo("America/Chicago"))
        for row in metadataconfig_info_df.collect():
            meta_runtimes_detail_dict = {'RunID' : run_id, 
                                         'RunJobName' : job_name,
                                         'RunJobId' : job_id,
                                         'RunSchema' : row['metaschema'],
                                         'RunTableName' : row['metatablename'],
                                        #  'RunStartTime' : datetime.now(ZoneInfo("America/Chicago")).strftime('%Y-%m-%d %H:%M:%S'),
                                         'RunTableLoadCompletedFlag' : "N"
                                        }
            meta_data.append(Row(**meta_runtimes_detail_dict))
        meta_df = spark.createDataFrame(meta_data)
        meta_df = meta_df.withColumn("RunStartTime", F.from_utc_timestamp(F.current_timestamp(), "America/Chicago"))

        try:
            meta_df.write.jdbc(url=secrets["persistent"]["url_p"], table='gw_cdc_meta_runtimes_pc_detail', mode="append", properties = persistent_post_db_options)
            logger.info(f"gw_cdc_meta_runtimes_pc_detail data initialized successfully for {meta_df.count()} tables")
        except Exception as e:
            logger.error(f"Error while initializing record into the gw_cdc_meta_runtimes_pc_detail: {e}")
            raise
    else:
        # For restart, only pick tables that were not completed in the previous run
        table_lst_query = f"SELECT runtablename as metatablename, runtableinsertcount FROM gw_cdc_meta_runtimes_pc_detail where runid = {run_id} and runtableloadcompletedflag = 'N' and runtablename in (SELECT metatablename FROM public.gw_cdc_metadata_config where metaschema = 'policycenter' and meta_worker = 2)"
        logger.info(f"table_lst_query : {table_lst_query}")

    table_lst_df = None  
    for attempt in range(1, max_retries + 1):
        try:
            table_lst_df = spark.read.jdbc(url=secrets["persistent"]["url_p"],table=f"({table_lst_query}) AS table_lst",  properties=persistent_post_db_options)
            break
        except Exception as e:
            logger.error(f"Attempt {attempt}: Error while retriving list of table names from the gw_cdc_meta_runtimes_pc_detail table: {str(e)}")
            if attempt < max_retries:
                logger.error(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached.")
                raise
    
    # Collect all required columns at once and Build the future map using the collected data
    table_data = table_lst_df.select("metatablename",  "runtableinsertcount").collect()
    future_to_table = {executor.submit(process_table, row["metatablename"] ,run_id, prev_run_start_time, curr_run_start_time, row["runtableinsertcount"]): row["metatablename"] for row in table_data}

    for future in as_completed(future_to_table):
        table_name = future_to_table[future]
        try:
            future.result()
        except Exception as e:
            logger.error(f"Error occurred for table {table_name}: {e}")