from pyspark.sql import SparkSession
from pathlib import Path

 
def convert_data_to_parquet(input_data, out_data, spark_):
    df = spark_.read.csv(input_data, header=True)
    df.write.parquet(out_data, mode='overwrite')
    print(f'Filename {input_data} have been convert to parquet in path {out_data}')


spark = SparkSession \
    .builder \
    .appName("CSV Data Conversion to Parquet") \
    .getOrCreate()

csv_files = ["hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/revgecoding.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/LA_income_2015.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/LA_income_2017.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/LA_income_2019.csv",
             "hdfs://master:9000/home/ubuntu/assignment/data/LA_income_2021.csv"]

for file_name in csv_files:
    out_filename = Path(file_name).stem
    print(out_filename)
    out_filename = f'hdfs://master:9000/home/ubuntu/assignment/data/{out_filename}.parquet'
    convert_data_to_parquet(file_name, out_filename, spark)
