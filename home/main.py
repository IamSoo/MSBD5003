import sys
import config
import os
from utils.S3Utils import S3Utils


def main(yamlFile):
	print(config.cfg["aws"])
	#connectToAWS(config.cfg)
	connectToAWS()



def connectToAWS():
	s3utils = S3Utils()
	s3Client = s3utils.createS3Client(config.cfg["aws"]["bucket_name"],
					  config.cfg["aws"]["aws_access_key_id"],
					  config.cfg["aws"]["aws_secret_access_key"],config.cfg["aws"]["region"])

	print("***** Starting to Download files and Upload to S3 Bucket ******")

	fileDownLoadAndUpload(s3Client)

def fileDownLoadAndUpload(self,s3Client):
	# Download and Upload all files
	s3utils = S3Utils()
	s3utils.downloadDataUploadToBucket(s3Client,
									   config.cfg["aws"]["bucket_name"],
									   config.cfg["api-urls"]["airlines_url"],
									   "data/airlines.json")

	s3utils.downloadDataUploadToBucket(s3Client,
									   config.cfg["aws"]["bucket_name"],
									   config.cfg["api-urls"]["airports_url"],
									   "data/airports.json")

	s3utils.downloadDataUploadToBucket(s3Client,
									   config.cfg["aws"]["bucket_name"],
									   config.cfg["api-urls"]["countries_url"],
									   "data/countries.json")

	s3utils.downloadDataUploadToBucket(s3Client,
									   config.cfg["aws"]["bucket_name"],
									   config.cfg["api-urls"]["airplanes_url"],
									   "data/airplanes.json")


if __name__=='__main__':
	#print(sys.argv[1])

	currentFodler = os.path.abspath(os.path.dirname(__file__))
	#print(currentFodler)
	sys.path.append(currentFodler)
	path = os.path.join(currentFodler, sys.argv[1])
	main(path)