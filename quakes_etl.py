import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('quakes_etl')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

#load the dataset
df_load = spark.read.csv("D:\\temp\\Earth Quake\\database.csv", header=True, inferSchema=True)

#dop unnecessary fields
lst_dropped_columns = ['Depth Error', 'Time', 'Depth Seismic Stations', 'Seismic Stations', 'Magnitude  Error', 'Magnitude Seismic Stations',
                       'Azimuthal Gap', 'Root Mean Square', 'Source', 'Location Source',' Magnitude Source', 'Status', 
                       'Magnitude Error', 'Horizontal Distance', 'Horizontal Error', 'Magnitude Source']

df_load = df_load.drop(*lst_dropped_columns)

#add year to the dataframe
df_load = df_load.withColumn('Year', year(to_timestamp('Date', 'dd/MM/yyyy')))

#calculate earth quake frequency per year
df_quake_freq = df_load.groupBy('Year').count()

# Calcualte max and avg magnitude per year
df_max = df_load.groupBy('Year').max('Magnitude').withColumnRenamed('max(Magnitude)', 'Max_Magnitude')
df_avg = df_load.groupBy('Year').avg('Magnitude').withColumnRenamed('avg(Magnitude)', 'Avg_Magnitude')

# join the calculated field to df_quake_freq
df_quake_freq = df_quake_freq.join(df_avg, ['Year']).join(df_max, ['Year'])

#remove nulls
df_quake_freq.dropna()
df_load.dropna()

#build the collections in Mongodb
df_load.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quakes').save()

df_quake_freq.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.quake_freq').save()

print(df_quake_freq.show(5))
print(df_load.show(5))

print("INFO: Job ran successfully")