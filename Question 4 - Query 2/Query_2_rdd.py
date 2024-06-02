from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, when
import time


def get_time_period_of_day(hour_):
    if 500 <= hour_ <= 1159:
        return 'MORNING'
    elif 1200 <= hour_ <= 1659:
        return 'AFTERNOON'
    elif 1700 <= hour_ <= 2059:
        return 'EVENING'
    elif 2100 <= hour_ <= 2359 or 0 <= hour_ <= 459:
        return 'NIGHT'
    else:
        return 'UNKNOWN'


def get_time_of_day(tuples):
    tuple_time_of_day = tuples[-1]
    return tuple_time_of_day, 1  # Return a tuple with the time of day period and count 1


# Create a Spark session
spark = SparkSession.builder \
    .appName("Query 2 RDD execution from csv") \
    .getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv', header=True)
df2 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2019_present.csv', header=True)

start_time = time.time()

rdd_1 = df1.rdd
rdd_2 = df2.rdd

# Remove the header names
header_1 = rdd_1.first()
rdd_1 = rdd_1.filter(lambda line: line != header_1)

header_2 = rdd_2.first()
rdd_2 = rdd_2.filter(lambda line: line != header_2)

# Union the two RDDs into a single one

rdd_union = rdd_1.union(rdd_2).distinct()

street_rdd = rdd_union.filter(lambda row: row['Premis Desc'] == 'STREET').distinct()

rdd_time_assign_map = street_rdd.map(lambda x: (tuple(x) + (get_time_period_of_day(int(x[3])), )))

rdd_count_time_of_day_rdd = rdd_time_assign_map.map(get_time_of_day).reduceByKey(lambda x, y: x + y)
sorted_count_time_of_day = rdd_count_time_of_day_rdd.sortBy(lambda x: x[1], ascending=False)

result_ = sorted_count_time_of_day.collect()
print("Time of Day\t\tCount")
for time_of_day, count in result_:
    print(f"{time_of_day}\t\t{count}")

total_time = time.time() - start_time
print(f'Execution time: {total_time} seconds')

spark.stop()

