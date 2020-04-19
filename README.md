# Earthquake Prediction Dashboard
![](dashboard.PNG?raw=true)
For interactive visualizations visit the following link:
https://public.tableau.com/profile/shahed.anzarus.sabab#!/vizhome/EarthQuakePrediction/Dashboard1

The objective is to report the prediction of the earthquake from the historical data. A machine learning model is trained with historical data of the world related to earthquakes from 1965-2016. The data includes geographical location and magnitude of the earthquakes (23.5k samples). The model predicts earthquake magnitude for the year of 2017. Finally, a dashboard is created to visualize the prediction in addition to the historical analysis on the data.
<br/>
• The data preprocessing and analysis is done using PySpark.<br/>
• MongoDB is used for storing the processed data.<br/>
• MLlib is used for training and prediction.<br/>
• Finally, Tableau is used for reporting the results of the analysis.<br/>

# File Description
• quakes.etl extracts the data, preprocess it and stores the processed data as MongoDB collections.<br/>
• quakes.ml loads the processed data from the server, trains regression model (i.e., Random Forest Regressor), predicts on test data and saves the prediction results as a new collection into the MongoDB server.<br/>
• Earth Quake.twb loads all the data from the server, create different visualizations and presents it on a dashboard.<br/>

# How to run
♣ Check required.txt for required installations and plugins.<br/>
1. Run quakes.etl using the following command from the cmd. 
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.1 quakes_etl.py<br/>
2. Run quakes.ml using the following command from the cmd.
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:2.4.1 quakes_ml.py<br/>
3. Create a system DSN (i.e., saved configuration that describes a database connection to be used by an ODBC driver) for MongoDB using ODBC Data Sources.<br/>
https://docs.mongodb.com/bi-connector/master/tutorial/create-system-dsn/<br/>
4. Load the data in Tableau Desktop usign data source as Other Databases (ODBC).
Use the created DSN and connect.
