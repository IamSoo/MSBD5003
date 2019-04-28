import boto3
import pandas as pd
import urllib.request



class S3Utils:
    def createS3Client(self,bucket,accessId,secret,region):
        client = boto3.resource("s3",
                                region_name=region,
                                aws_access_key_id=accessId,
                                aws_secret_access_key=secret)
        return client

    def downloadDataUploadToBucket(self,client,bucketName,fileUrlToDownload,filePathToUpload):
        data = self.downloadDataFromWeb(fileUrlToDownload)
        #print("Downloaded data is {}".format(data))
        client.Object(bucketName,filePathToUpload).put(Body=data)
        print("File {} is uploaded to bucket ".format(fileUrlToDownload))

    def downloadDataFromWeb(self,fileUrlToDownload):
        with urllib.request.urlopen(fileUrlToDownload) as response:
            print("Source File {} is downloaded".format(fileUrlToDownload))
            return response.read()
