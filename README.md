# sample etl pyspark scirpt

process
1. read json rawdata files (because firehose buffer delay)
2. filer target utc time and make table
3. make sql script for each data table (add partition key, casting data, filtering) 
