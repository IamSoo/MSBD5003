import boto3

aws_access_key_id="AKIAYHUHQBF5LHCHYOED"
aws_secret_access_key="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"
region="us-east-2"
bucket_name="5003-project"

client = boto3.resource("dynamodb",
                        region_name=region,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

table = client.Table("test")
table.put_item(Item={
        "date" : "20190428",
        "data" : "{'name':'soo','country':'CN'}"
})