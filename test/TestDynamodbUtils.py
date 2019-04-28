import boto3



def getDynamodbClient(self,region,aws_access_key_id,aws_secret_access_key):
    client = boto3.resource("dynamodb",
                            region_name=region,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    return client


def insertData(self,client,tableName,keyDate,data):
    table = client.Table(tableName)
    table.put_item(Item={
        "date": keyDate,
        "data": data})
    print("Data with key value as {} inserted successfully!!!".format(keyDate))
