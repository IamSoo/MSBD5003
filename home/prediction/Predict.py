import boto3
import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


conf = SparkConf()\
    .setAppName("Spark Application")\
    .setMaster("local")\
    .set("spark.executor.memory", "1g")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


def getCSVFilePathFromTxtFile(file):
    indexOfDot = file.rfind(".")
    return file[:indexOfDot] +".csv"

file="https://s3-ap-southeast-1.amazonaws.com/5003-project/data/airlines.txt"
df = pd.read_json(file)
print(df)
df.to_csv("test.csv", index=True, encoding='utf-8')

sdf = sqlContext.createDataFrame(df)




