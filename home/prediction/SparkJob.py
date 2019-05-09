import pandas as pd
import requests
import config
import urllib.request, json
from datetime import date
import boto3
import os
import yaml

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

class SparkJob():
    def runSparkJob(self):
        conf = SparkConf() \
            .setAppName("Spark Application") \
            .setMaster("local") \
            .set("spark.executor.memory", "1g")

        #sc = SparkContext("local", "App Name", pyFiles=['SparkJob.py', 'lib.zip', 'app.egg'])

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)
        file_flights = "https://s3-ap-southeast-1.amazonaws.com/5003-project/data/flights.json"
        response = json.loads(requests.get(file_flights).text)
        flights_df = pd.io.json.json_normalize(response)
        #print(df)
        sdf = sqlContext.createDataFrame(flights_df)
        filtered_flight_df = sdf.select("`departure.iataCode`","`flight.number`") \
            .withColumnRenamed("flight.number", "flightNumber") \
            .withColumnRenamed("departure.iataCode", "departureCity") \
            .filter("flightNumber!=''") \
            .filter("departureCity='HKG'")

        file_airplanes = "https://s3-ap-southeast-1.amazonaws.com/5003-project/data/airplanes.json"

        planes_pd_df = pd.read_json(file_airplanes)
        planes_df = sqlContext.createDataFrame(planes_pd_df)

        final_df = planes_df.join(filtered_flight_df, planes_df.lineNumber == filtered_flight_df.flightNumber,'inner')

        final_df.createOrReplaceTempView('joined_table')

        reslt_df = sqlContext.sql("select distinct airplaneId , deliveryDate, modelCode , planeAge , planeOwner, planeSeries from joined_table \
        where planeAge = (select max(planeAge) from joined_table)") \
            .orderBy('planeAge', ascending=False)

        strJsonData = reslt_df.toJSON().collect()

        self.callDynamoDbInsert(strJsonData)

    def callDynamoDbInsert(self,dataJson):
        today = date.today()
        #dataJson["sysdate"]= today.strftime('%Y%m%d')
        print(today.strftime('%Y%m%d'))
        config = self.getConfig()
        self.insertDataIntoDb(config.cfg["aws"]["region"],
                                 config.cfg["aws"]["aws_access_key_id"],
                                 config.cfg["aws"]["aws_secret_access_key"],
                                 dataJson)

    def createS3Client(self):
        config = self.getConfig()
        client = boto3.resource("dynamodb",
                        region_name=config.cfg["aws"]["region"],
                        aws_access_key_id=config.cfg["aws"]["aws_access_key_id"],
                        aws_secret_access_key=config.cfg["aws"]["aws_secret_access_key"])
        return client;

    def insertDataIntoDb(self,region,aws_access_key_id,aws_secret_access_key,jsonStr):
        client = self.createS3Client(region,aws_access_key_id,aws_secret_access_key)
        table = client.Table("5003-data")
        table.put_item(Item=jsonStr)
        print("Data has been pushed to dynamodb")

    def getConfig(self):
        currentFodler = os.path.abspath(os.path.dirname(__file__))
        with open(os.path.join(currentFodler, "sparkConfig.yml"), 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        return config


if __name__=='__main__':
    job = SparkJob()
    job.runSparkJob()



