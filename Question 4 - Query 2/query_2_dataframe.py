from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank, when, desc
import time

spark = SparkSession \
    .builder \
    .appName("Query 2 Dataframe execution from csv") \
    .getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv', header=True)
df2 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.csv', header=True)

start_time = time.time()

# Union all the data
df_merged_csv = df1.union(df2).dropDuplicates()

df_merged_csv = df_merged_csv.withColumn('Date Rptd', to_timestamp(df_merged_csv['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('DATE OCC', to_timestamp(df_merged_csv['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('Vict Age', col("Vict Age").cast('int'))
df_merged_csv = df_merged_csv.withColumn('LAT', col("LAT").cast('double'))
df_merged_csv = df_merged_csv.withColumn('LON', col("LON").cast('double'))

df_street_results = df_merged_csv.filter(col('Premis Desc') == 'STREET')

# Partition crime by the time of the time of the crime
condition_morning = ((col('TIME OCC') >= "0500") & (col('TIME OCC') <= "1159"))
condition_afternoon = ((col('TIME OCC') >= "1200") & (col("TIME OCC") <= "1659"))
condition_evening = ((col('TIME OCC') >= "1700") & (col('TIME OCC') <= "2059"))
condition_night = ((col('TIME OCC') >= "2100") | (col('TIME OCC') <= "0459"))

df_crime_time_of_date = df_street_results.withColumn('Crime_time_of_date', when(condition_morning, "MORNING")
                                                     .when(condition_afternoon, 'AFTERNOON')
                                                     .when(condition_evening, 'EVENING')
                                                     .when(condition_night, 'NIGHT')
                                                     .otherwise('OTHERWISE'))

# Select all the rows from the column Crime_time_of_date
df_select_crime_time_of_date = df_crime_time_of_date.select(*['Crime_time_of_date'])

df_final_result = (df_select_crime_time_of_date.groupBy('Crime_time_of_date').count()
                   .orderBy(col('count').desc()))

df_final_result.printSchema()

df_final_result.show(10)
total_time = time.time() - start_time

print(f'Execution time: {total_time}')

spark.stop()
