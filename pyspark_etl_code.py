import sys, datetime
from functools import reduce
from itertools import islice

from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import *


if __name__ == "__main__":

    # process
    # 1. read json rawdata files (because firehose buffer delay)
    # 2. filer target utc time and make table
    # 3. make sql script for each data table (add partition key, casting data, filtering) 


    
    spark = SparkSession.builder.appName("StatsAnalyzer")\
            .enableHiveSupport().config("hive.exec.dynamic.partition", "true")\
            .config("hive.exec.dynamic.partition.mode", "nonstrict")\
            .getOrCreate()
    sqlContext = SQLContext(spark.sparkContext)
    
    # raw_data_soruce
    raw_data_bucket = "sample_s3_bucket"
    raw_data_dir = "rawdata"
    raw_data_detail = "action_info"
    
    # data catalog schema_info
    schema_db = "rawdata_test"
    
    # target user trigger time
    airflow_execution_time = sys.argv[1]  
    # ex) airflow_execution_time = '2022-03-07T02:10:07+00:00'
    #     it means we need to etl rawdata triggered by user at 2022-03-07 02 utc time&hour
    
    # parse s3 date dir and df & view creation
    def get_file_dir(date_time_str):
        date_time_str = date_time_str.split('+')[0]
        date_time_obj = datetime.datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S")
        prev_date_time_obj = date_time_obj - datetime.timedelta(hours = 1)
        
        date_dir_list = [prev_date_time_obj.strftime("%Y-%m-%dT%H:%M:%S"), date_time_obj.strftime("%Y-%m-%dT%H:%M:%S")]
        
        final_date_dir = []
        final_date_condition = []
        
        for date_dir in date_dir_list:
            date_info, hour_info = date_dir.split('T')
            default_dir = f"s3://{raw_data_bucket}/{raw_data_dir}/"
            date_dir = '/'.join(date_info.split('-'))
            hour_dir = hour_info.split(':')[0]
            
            final_date_dir.append(default_dir + date_dir + "/" + hour_dir + "/")
            final_date_condition.append(date_info + "-" + hour_dir)
            
        return final_date_dir, final_date_condition[-1]
    
        
    date_dir_list, date_condition = get_file_dir(airflow_execution_time)
    
    
    
    print("Date dir list : " + str(date_dir_list))
    print("Date condition : " + str(date_condition))
    
    # union two utc rawdata foler because of kinesis firehose buffer delay
    rawdata_df_1 = reduce(DataFrame.unionAll,  [spark.read.json(date_dir) for date_dir in date_dir_list])
    rawdata_df_2 = rawdata_df_1.filter(rawdata_df_1.base_date.contains(date_condition) )
    
    print('Data filtered from ' + str(rawdata_df_1.count()) + ' to ' + str(rawdata_df_2.count()))
    
    rawdata_df_2.createOrReplaceTempView("rawdata")
    print('Data view "rawdata" created')
    
    
    # get target table, raw data, and querying
    rawdata_column = rawdata_df_2.columns
    rawdata_column.remove(raw_data_detail)
    print('Rawdata Columns : ' + str(rawdata_column))
    
    df_list = sqlContext.sql(f"""show tables from {schema_db}""")
    table_name_list = list(set(df_list.filter(df_list.isTemporary == False).select("tableName").rdd.flatMap(lambda x: x).collect()))
    print('Collecting table name : ' + str(table_name_list))
    
    def create_hive_sql_query(schema_db, table_name, base_date, utc_hour):
        
        # schema_db : rawdata_test
        # table_name : client_user_login
        
        # schema_list : target table columns
        # schema_type : target table types
        # rawdata_column : raw data columns
        
        hive_sql_query = ""
        hive_sql_query = hive_sql_query + f"insert overwrite table {schema_db}.{table_name}\n"
        hive_sql_query = hive_sql_query + f"PARTITION(base_date = '{base_date}', utc_hour = {utc_hour})\n"
        hive_sql_query = hive_sql_query + f"select\n"
    
        for c_idx, s_column in enumerate(schema_column):
            last_str = '' if c_idx == len(schema_column) - 1 else ','
            
            if s_column == 'base_dt':
                hive_sql_query = hive_sql_query + f"\tcast(base_date as string) as {s_column}{last_str}\n"
            elif s_column == 'number_p':
                hive_sql_query = hive_sql_query + f"\tcast({raw_data_detail}.number as string) as {s_column}{last_str}\n"
            elif s_column in rawdata_column:
                hive_sql_query = hive_sql_query + f"\tcast({s_column} as {schema_type[c_idx]}) as {s_column}{last_str}\n"
            else:
                hive_sql_query = hive_sql_query + f"\tcast({raw_data_detail}.{s_column} as {schema_type[c_idx]}) as {s_column}{last_str}\n"
        hive_sql_query = hive_sql_query + f"from rawdata\n"
        hive_sql_query = hive_sql_query + f"where event_name = '{table_name}'\n"
        
        return hive_sql_query
        
    for table_name in table_name_list:
        print('Current table name : ' + str(table_name))
        
        schema_info = sqlContext.sql(f"desc {schema_db}.{table_name}")
    
        schema_list = schema_info.select("col_name").rdd.flatMap(lambda x: x).collect()
        schema_type_list = schema_info.select("data_type").rdd.flatMap(lambda x: x).collect()
    
        schema_column = schema_list[:schema_list.index("# Partition Information") - 2]
        schema_type = schema_type_list[:schema_list.index("# Partition Information") - 2]
        
        hive_sql = create_hive_sql_query(schema_db, table_name, date_condition[:-3], int(date_condition[-2:]))
        print('hive sql script : ' + str(hive_sql))
        sqlContext.sql(hive_sql)
    
    
