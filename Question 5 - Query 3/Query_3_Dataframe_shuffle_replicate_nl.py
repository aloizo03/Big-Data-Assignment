from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, to_timestamp, col, year, month, desc, rank, regexp_replace, when, broadcast
import time

spark = SparkSession.builder \
    .appName("Query 3 Dataframe execution from csv") \
    .getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/los_angeles_crimes_2010_2019.csv', header=True)

df_income_2015 = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/LA_income_2015.csv', header=True)
df_rev_ge_coding = spark.read.csv('hdfs://master:9000/home/ubuntu/assignment/data/revgecoding.csv', header=True)

start_time = time.time()

df1 = df1.withColumn('Date Rptd', to_timestamp(df1['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df1 = df1.withColumn('DATE OCC', to_timestamp(df1['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df1 = df1.withColumn('Vict Age', col('Vict Age').cast('int'))
df1 = df1.withColumn('LAT', col("LAT").cast('double'))
df1 = df1.withColumn('LON', col("LON").cast('double'))

df_income_2015 = df_income_2015.withColumn('Zip Code', col('Zip Code').cast('int'))
df_income_2015 = df_income_2015.withColumn("Estimated Median Income",
                                           regexp_replace(df_income_2015["Estimated Median Income"], ",", ""))
df_income_2015 = df_income_2015.withColumn("Estimated Median Income",
                                           regexp_replace(df_income_2015["Estimated Median Income"], "\$", "").cast(
                                               'float'))

df_rev_ge_coding = df_rev_ge_coding.withColumn('LAT', col("LAT").cast('double'))
df_rev_ge_coding = df_rev_ge_coding.withColumn('LON', col("LON").cast('double'))
df_rev_ge_coding = df_rev_ge_coding.withColumn('ZIPcode', col('ZIPcode').cast('int'))
df_rev_ge_coding = df_rev_ge_coding.withColumnRenamed('ZIPcode', 'Zip Code')

df1 = df1.filter(col("Vict Descent").isNotNull())

crime_data_per_zip = df1.join(df_rev_ge_coding.hint('shuffle_replicate_nl'),
                              (df_rev_ge_coding['LAT'] == df1['LAT']) & (df_rev_ge_coding['LON'] == df1['LON']),
                              "inner") \
    .select("Zip Code", "Vict Descent")

crime_data_per_zip = crime_data_per_zip.groupBy("Zip Code", "Vict Descent").count()

# Filter out ZIP codes with no victim descent
crime_data_per_zip = crime_data_per_zip.filter(col("Vict Descent").isNotNull())

sorted_LA_income = (df_income_2015.filter(df_income_2015["Community"].like("%Los Angeles (%"))
                    .orderBy(col("Estimated Median Income").desc()))

# Get 3 top and bottom ZIP codes by income
top_income_zip = sorted_LA_income.limit(3).select("Zip Code")
# sorted_LA_income = sorted_LA_income.orderBy(col("Estimated Median Income").asc())
bottom_income_zip = sorted_LA_income.orderBy(col("Estimated Median Income"), ascending=True).limit(3).select("Zip Code")

# Join with crime data per zip code
top_victims = top_income_zip.join(crime_data_per_zip.hint('shuffle_replicate_nl'), "Zip Code").orderBy(col("count").desc())
bottom_victims = bottom_income_zip.join(crime_data_per_zip.hint('shuffle_replicate_nl'), "Zip Code").orderBy(col("count").desc())

conditions = (when(col('Vict Descent') == "A", "Other Asian")
              .when(col('Vict Descent') == "B", "Black")
              .when(col('Vict Descent') == "C", "Chinese")
              .when(col('Vict Descent') == "D", "Cambodian")
              .when(col('Vict Descent') == "F", "Filipino")
              .when(col('Vict Descent') == "G", "Guamanian")
              .when(col('Vict Descent') == "H", "Hispanic/Latin/Mexican")
              .when(col('Vict Descent') == "I", "Hispanic/Latin/Mexican")
              .when(col('Vict Descent') == "J", "Japanese")
              .when(col('Vict Descent') == "K", "Korean")
              .when(col('Vict Descent') == "L", "Laotian")
              .when(col('Vict Descent') == "O", "Other")
              .when(col('Vict Descent') == "P", "Pacific Islander")
              .when(col('Vict Descent') == "S", "Samoan")
              .when(col('Vict Descent') == "U", "Hawaiian")
              .when(col('Vict Descent') == "V", "Vietnamese")
              .when(col('Vict Descent') == "W", "White")
              .when(col('Vict Descent') == "X", "Unknown")
              .when(col('Vict Descent') == "Z", "Asian Indian")
              .otherwise("OTHERWISE"))

top_income_victims_res = top_victims.withColumn("Vict Descent", conditions).orderBy('Zip Code', col('count').desc())
bottom_income_victims_res = bottom_victims.withColumn("Vict Descent", conditions).orderBy('Zip Code', col('count').desc())

top_income_victims_res.show(200)
bottom_income_victims_res.show(200)
total_time = time.time() - start_time

print(f'Execution time: {total_time} seconds')
spark.stop()

