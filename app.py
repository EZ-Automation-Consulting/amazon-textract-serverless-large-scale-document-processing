import boto3
import time
import csv
import json

# Document
s3BucketName = "textract-console-us-east-2-2987cd7c-e629-4de2-ab04-11e0e6bbf3fe"
documentName = "_1438556735_coi.pdf"

# Amazon Textract client
textract = boto3.client('textract')

# Call Amazon Textract
data = textract.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': documentName
        }
    },
    FeatureTypes=[
        'TABLES','FORMS',
    ],
    NotificationChannel={
        'SNSTopicArn': 'arn:aws:sns:us-east-2:929835491811:AmazonTextract',
        'RoleArn': 'arn:aws:iam::929835491811:role/TextractRole'
    }
)

# Needs to be replaced with https://docs.aws.amazon.com/textract/latest/dg/api-async.html SQS Queue Monitoring
time.sleep(60)

response = textract.get_document_analysis(
    JobId=data['JobId'],
    MaxResults=200
)

 
with open('data.txt', 'w') as outfile:
    json.dump(response, outfile)

# # Print detected text
# for item in response["Blocks"]:
#     if item["BlockType"] == 'KEY_VALUE_SET':
#         print ('\033[94m' +  item + '\033[0m')