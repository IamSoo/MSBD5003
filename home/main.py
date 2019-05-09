import sys
import config
import os
from utils.S3Utils import S3Utils

def main():
	#1
	s3Client = connectToAWS()
	#2
	fileDownLoadAndUpload(s3Client)


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

if __name__=='__main__':
	currentFodler = os.path.abspath(os.path.dirname(__file__))
	#print(currentFodler)
	sys.path.append(currentFodler)
	#path = os.path.join(currentFodler, sys.argv[1])
	main()