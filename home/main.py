import sys
import config
import os
from utils.S3Utils import S3Utils
from prediction.SparkJob import SparkJob
from utils.DynamodbUtils import DynamoDbUtils


def main(yamlFile):
	print(config.cfg["aws"])
	#1
	s3Client = connectToAWS()
	#2
	fileDownLoadAndUpload(s3Client)
	#3
	dataJson = callSparkJob()
	#4
	callDynamoDbInsert(dataJson)


def connectToAWS():
	s3utils = S3Utils()
	s3Client = s3utils.createS3Client(config.cfg["aws"]["bucket_name"],
					  config.cfg["aws"]["aws_access_key_id"],
					  config.cfg["aws"]["aws_secret_access_key"],config.cfg["aws"]["region"])

	print("***** Starting to Download files and Upload to S3 Bucket ******")
	return s3Client

def fileDownLoadAndUpload(s3Client):
	# Download and Upload all files
	s3utils = S3Utils()
	s3utils.downloadDataUploadToBucket(s3Client,
									   config.cfg["aws"]["bucket_name"],
									   config.cfg["api-urls"]["flights_url"],
									   "data/flights.json")

def callSparkJob():
	sparkJobObject = SparkJob()
	sparkJobObject.runSparkJob()


def callDynamoDbInsert(dataJson):
	dbutils = DynamoDbUtils()
	dbutils.insertDataIntoDb(config.cfg["aws"]["region"],
							 config.cfg["aws"]["aws_access_key_id"],
							 config.cfg["aws"]["aws_secret_access_key"],
							 dataJson)

if __name__=='__main__':
	currentFodler = os.path.abspath(os.path.dirname(__file__))
	#print(currentFodler)
	sys.path.append(currentFodler)
	path = os.path.join(currentFodler, sys.argv[1])
	main(path)