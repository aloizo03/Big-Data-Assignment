from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank
import time


def execute_sql_query(spark):
    SQL_Query = ("WITH year_month AS ( "
                 "  SELECT `DATE OCC`, YEAR(`DATE OCC`) AS `Year`, MONTH(`DATE OCC`) AS `Month` "
                 "  FROM Crime_Data_View), "
                 "rank_crimes AS ( "
                 "  SELECT `Year`, `Month`, COUNT(*) AS `CrimeCount`, "
                 "    RANK() OVER (PARTITION BY `Year` ORDER BY COUNT(*) DESC) AS `crime_rank` "
                 "  FROM year_month "
                 "  GROUP BY `Year`, `Month` "
                 "), "
                 "top_3_month_per_year AS ( "
                 "  SELECT `Year`, `Month`,`CrimeCount` AS `Total_Crimes`, `crime_rank` AS `Rank`"
                 "  FROM rank_crimes"
                 "  WHERE `crime_rank` <= 3) "
                 "SELECT * "
                 "FROM top_3_month_per_year ")

    spark.sql(SQL_Query).show(100)


spark = SparkSession \
    .builder \
    .appName("Query 1 SQL execution from parquet") \
    .getOrCreate()

df1 = spark.read.parquet('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.parquet', header=True)
df2 = spark.read.parquet('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.parquet', header=True)

start_time = time.time()

# Union all the data
df_merged_csv = df1.union(df2).dropDuplicates()


df_merged_csv = df_merged_csv.withColumn('Date Rptd', to_timestamp(df_merged_csv['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('DATE OCC', to_timestamp(df_merged_csv['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('Vict Age', col("Vict Age").cast('int'))
df_merged_csv = df_merged_csv.withColumn('LAT', col("LAT").cast('double'))
df_merged_csv = df_merged_csv.withColumn('LON', col("LON").cast('double'))

# Register Dataframe as Temporary View of SQL
df_merged_csv.createOrReplaceTempView('Crime_Data_View')

execute_sql_query(spark=spark)

spark.catalog.dropTempView("Crime_Data_View")
total_time = time.time() - start_time

print(f'Execution time: {total_time} seconds')
spark.stop()
