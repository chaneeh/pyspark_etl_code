import sys, datetime
from functools import reduce

from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, DataFrame 
from pyspark.sql.types import StringType
from pyspark.sql.functions import *


# convert date_time_str to s3_file_dir
def get_file_dir(date_time_str):
    date_time_str = date_time_str.split('+')[0]
    date_time_obj = datetime.datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S")
    prev_date_time_obj = date_time_obj - datetime.timedelta(hours = 1)
    
    date_dir_list = [prev_date_time_obj.strftime("%Y-%m-%dT%H:%M:%S"), date_time_obj.strftime("%Y-%m-%dT%H:%M:%S")]
    
    final_date_dir = []
    final_date_condition = ''
    final_condition = []
    
    for date_dir in date_dir_list:
        date_info, hour_info = date_dir.split('T')
        default_dir = f"s3://{raw_data_bucket}/{raw_data_dir}/"
        date_dir, hour_dir = '/'.join(date_info.split('-')), hour_info.split(':')[0]
        
        final_date_dir.append(default_dir + date_dir + "/" + hour_dir + "/")
        final_date_condition = date_info + "-" + hour_dir
        final_condition = [date_info, hour_dir]
        
    return final_date_dir, final_date_condition, final_condition


# create sql query for target table
def create_spark_sql_query(schema_db, table_name, table_columns, table_types, rawdata_column, raw_data_detail, base_date, utc_hour):
    
    # schema_db     : rawdata_test
    # table_name    : client_user_login
    # table_columns : ['base_dt', 'user_id', 'os', ...]
    # table_types   : ['DATE', 'STRING', 'STRING', ...]]
    
    spark_sql_query = ""
    spark_sql_query = spark_sql_query + f"insert overwrite table {schema_db}.{table_name}\n"
    spark_sql_query = spark_sql_query + f"PARTITION(base_date = date'{base_date}', utc_hour = {utc_hour})\n"
    spark_sql_query = spark_sql_query + f"select\n"

    for c_idx, s_column in enumerate(table_columns):
        last_str = '' if c_idx == len(table_columns) - 1 else ','
        
        if s_column in rawdata_column:
            spark_sql_query = spark_sql_query + f"\tcast({s_column} as {table_types[c_idx]}) as {s_column}{last_str}\n"
        else:
            spark_sql_query = spark_sql_query + f"\tcast({raw_data_detail}.{s_column} as {table_types[c_idx]}) as {s_column}{last_str}\n"
    spark_sql_query = spark_sql_query + f"from rawdata\n"
    spark_sql_query = spark_sql_query + f"where event_name = '{table_name}'\n"
    
    return spark_sql_query
    




if __name__ == "__main__":

    # process
    # 1. read json rawdata files (because firehose buffer delay)
    # 2. filer target utc time and make table
    # 3. make sql script for each data table (add partition key, casting data, filtering) 

    
    spark = SparkSession.builder.appName("pyspark_etl_code_sample")\
            .enableHiveSupport().config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .getOrCreate()
    sqlContext = SQLContext(spark.sparkContext)
    
    # raw_data_soruce
    raw_data_bucket = "sample_s3_bucket"
    raw_data_dir    = "rawdata"
    raw_data_detail = "action_info"
    
    # data catalog schema_info
    schema_db       = "rawdata_test"
    
    # target user trigger time
    # ex) airflow_execution_time = '2022-03-07T02:10:07+00:00' => etl user data on 2022/03/07 2pm 
    airflow_execution_time = sys.argv[1]  
    
    # get target s3 file dir      
    date_dir_list, date_condition, query_condition = get_file_dir(airflow_execution_time)
    
    # union two utc rawdata folder because of kinesis firehose buffer delay
    rawdata_df = reduce(DataFrame.unionAll,  [spark.read.json(date_dir) for date_dir in date_dir_list])
    rawdata_df = rawdata_df.filter(rawdata_df.base_dt.contains(date_condition))

    # create date table
    rawdata_df.createOrReplaceTempView("rawdata")

    print("Date dir list : " + str(date_dir_list))
    print("Date condition : " + str(date_condition))
    print('Rawdata Columns : ' + str(rawdata_df.columns))



    # collect table names
    df_list = sqlContext.sql(f"""show tables from {schema_db}""")
    table_name_list = list(set(df_list.filter(df_list.isTemporary == False).select("tableName").rdd.flatMap(lambda x: x).collect()))

    print('Collecting table name : ' + str(table_name_list))
    
    # create and execute spark sql query for each target table
    for table_name in table_name_list:
        print('Current table name : ' + str(table_name))
        
        schema_info = sqlContext.sql(f"desc {schema_db}.{table_name}")
    
        table_col_list = schema_info.select("col_name").rdd.flatMap(lambda x: x).collect()
        table_type_list = schema_info.select("data_type").rdd.flatMap(lambda x: x).collect()
    
        table_columns = table_col_list[:table_col_list.index("# Partition Information") - 2]
        table_types = table_type_list[:table_col_list.index("# Partition Information") - 2]
        
        spark_sql = create_spark_sql_query(schema_db, table_name, table_columns, table_types, rawdata_df.columns, raw_date_detail, query_condition[0], query_condition[1])
        print('create sql script : ' + str(spark_sql))
        sqlContext.sql(spark _sql)
    
    
