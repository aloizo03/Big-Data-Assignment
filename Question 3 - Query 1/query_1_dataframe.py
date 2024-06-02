from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank
import time


def execute_df_windowing(df):
    df_date_occ = df.select(*['DATE OCC'])
    df_year_month = df_date_occ.withColumn('Year', year('DATE OCC')) \
        .withColumn("Month", month("DATE OCC"))

    windowing = Window.partitionBy("Year").orderBy(col('count').desc())

    top_3_month_per_year = df_year_month.groupby("Year", "Month").count() \
        .withColumn("Rank", rank().over(windowing)) \
        .filter(col('Rank') <= 3)

    top_3_month_per_year.show(50)
    spark.stop()


spark = SparkSession \
    .builder \
    .appName("Query 1 dataframe execution from csv") \
    .getOrCreate()
df1 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv', header=True)
df2 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.csv', header=True)

start_time = time.time()

# Union all the data
df_merged_csv = df1.union(df2).dropDuplicates()

df_merged_csv = df_merged_csv.withColumn('Date Rptd', to_timestamp(df_merged_csv['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('DATE OCC', to_timestamp(df_merged_csv['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn("Vict Age", col("Vict Age").cast("int"))
df_merged_csv = df_merged_csv.withColumn("LAT", col("LAT").cast("double"))
df_merged_csv = df_merged_csv.withColumn("LON", col("LON").cast("double"))

execute_df_windowing(df_merged_csv)

total_time = time.time() - start_time
print(f'Execution time: {total_time} seconds')

spark.stop()
