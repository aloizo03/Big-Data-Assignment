import geopy.distance
from pyspark.sql import SparkSession
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Row
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank, regexp_replace, when, udf, avg, \
    count


# Calculate the distance between two points [lat1, long1], [lat2, long2] in km
def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


# Initialize Spark session
spark = SparkSession.builder.appName("Query4_RDD").getOrCreate()

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

police_stations_rdd = df_police_stations.select(["PREC", "DIVISION", "Y", "X"]).rdd
df_merged_rdd = df_merged_csv.select(['AREA ', 'LAT', 'LON']).rdd

df_merged_rdd = df_merged_rdd.map(lambda row: (row['AREA '], (row['LAT'], row['LON'])))
police_stations_rdd = police_stations_rdd.map(lambda row: (row['PREC'], (row['Y'], row['X'], row['DIVISION'])))

# Join the data using the key-value pairs
joined_rdd = df_merged_rdd.join(police_stations_rdd)

# Calculate distances and transform to Row objects
distance_rdd = joined_rdd.map(lambda row: Row(
    crime_LAT=row[1][0][0],
    crime_LON=row[1][0][1],
    station_LAT=row[1][1][0],
    station_LON=row[1][1][1],
    division=row[1][1][2],
    distance=get_distance(row[1][0][0], row[1][0][1], row[1][1][0], row[1][1][1])
))

# Convert the RDD to a DataFrame
distance_df = spark.createDataFrame(distance_rdd)

# Calculate average distance by division
average_distance_by_division = distance_df.groupBy('division').agg(
    avg('distance').alias('average_distance'),
    count("*").alias("number_of_incidents")
).orderBy(desc("number_of_incidents"))

# Display the results
average_distance_by_division.show(50)

total_time = time.time() - start_time
print(f'Execution time: {total_time} seconds')

# Stop Spark session
spark.stop()
