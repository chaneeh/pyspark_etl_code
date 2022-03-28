# sample etl pyspark script

process
1. read json rawdata files (because firehose buffer delay)
2. filter target utc time and make table
3. make sql script for each data table (add partition key, casting data, filtering) 
