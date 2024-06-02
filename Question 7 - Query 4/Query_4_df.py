import geopy.distance
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DoubleType, MapType
import time
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank, regexp_replace, when, udf, avg, \
    count
import time


def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


spark = SparkSession \
    .builder \
    .appName("Query 4 Dataframe execution") \
    .getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv', header=True)
df2 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.csv', header=True)

df_police_stations = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/LAPD_Police_Stations.csv',
                                    header=True)

start_time = time.time()

df_merged_csv = df1.union(df2).dropDuplicates()

df_merged_csv = df_merged_csv.withColumn('Date Rptd', to_timestamp(df_merged_csv['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('DATE OCC', to_timestamp(df_merged_csv['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df_merged_csv = df_merged_csv.withColumn('Vict Age', col("Vict Age").cast('int'))
df_merged_csv = df_merged_csv.withColumn('AREA ', col("Vict Age").cast('int'))
df_merged_csv = df_merged_csv.withColumn('LAT', col("LAT").cast('double'))
df_merged_csv = df_merged_csv.withColumn('LON', col("LON").cast('double'))
df_merged_csv = df_merged_csv.withColumn('Weapon Used Cd', col("Weapon Used Cd").cast('int'))

df_police_stations = df_police_stations.withColumn('PREC', col('PREC').cast('int'))
df_police_stations = df_police_stations.withColumn('X', col('X').cast('double'))
df_police_stations = df_police_stations.withColumn('Y', col('Y').cast('double'))

df_merged_csv.filter((col('Weapon Used Cd') >= 100) &
                     (col('Weapon Used Cd') < 200) &
                     (col('LAT') != 0.) &
                     (col('LON') != 0.))

df_crimes_in_la_join = df_merged_csv.join(df_police_stations, df_merged_csv['AREA '] == df_police_stations['PREC'])

crimes_in_la = df_crimes_in_la_join.select(
    col('LAT'),
    col('LON'),
    col('X'),
    col('Y'),
    col('DIVISION')
)

get_distance_udf = udf(get_distance, DoubleType())

crimes_in_la_distance_from_div = crimes_in_la.withColumn('distance', get_distance_udf(crimes_in_la['LAT'],
                                                                    crimes_in_la['LON'],
                                                                    crimes_in_la['Y'],
                                                                    crimes_in_la['X']))


average_distance_by_division = crimes_in_la_distance_from_div.groupBy('DIVISION').agg(
    avg('distance').alias('average_distance'),
    count("*").alias("number_of_incidents")
).orderBy(desc("number_of_incidents"))

average_distance_by_division.show(20)

total_time = time.time() - start_time

print(f'Execution time: {total_time} seconds')
spark.stop()
