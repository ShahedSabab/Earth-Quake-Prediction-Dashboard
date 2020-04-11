import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np

spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('quakes_ml')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1')\
    .getOrCreate()

#load the dataset
df_test = spark.read.csv("D:\\temp\\Earth Quake\\query.csv", header=True, inferSchema=True)

#read the training data from mongo
df_train = spark.read.format('mongo')\
    .option('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/Quake.quakes').load()

#filter test data and remove unnecessary attributes
df_test_clean = df_test['time', 'latitude', 'longitude', 'mag', 'depth']

#rename test to match with train data
df_test_clean = df_test_clean.withColumnRenamed("time", "Date")\
    .withColumnRenamed("latitude", "Latitude")\
    .withColumnRenamed("longitude", "Longitude")\
    .withColumnRenamed("mag", "Magnitude")\
    .withColumnRenamed("depth", "Depth")

#traing-test data
df_testing = df_test_clean['Latitude', 'Longitude', 'Magnitude', 'Depth']
df_training = df_train['Latitude', 'Longitude', 'Magnitude', 'Depth']

#dropping null form the dataset
df_training = df_training.dropna()
df_testing = df_testing.dropna()

#assembler 
assembler = VectorAssembler(inputCols=['Latitude', 'Longitude', 'Depth'], outputCol = 'features')

model = RandomForestRegressor(featuresCol='features', labelCol='Magnitude')

#pipeline 
pipeline = Pipeline(stages=[assembler, model])

#train_model
model = pipeline.fit(df_training)

#make prediction
pred = model.transform(df_testing)

#evaluate
evaluator = RegressionEvaluator(labelCol='Magnitude', predictionCol='prediction', metricName = 'rmse')
rmse = evaluator.evaluate(pred)

#create the prediction dataset
df_pred_results = pred['Latitude', 'Longitude', 'prediction']

#rename prediction column
df_pred_results = df_pred_results.withColumnRenamed('prediction', 'Pred_Magnitude')

#add more column to df_pred_results
df_pred_results = df_pred_results.withColumn('Year', lit(2017)).withColumn('RMSE', lit(rmse))

#save prediction dataset to mongodb
df_pred_results.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Quake.pred_results').save()

print(df_pred_results.show(3))

print("INFO: Job ran successfully")