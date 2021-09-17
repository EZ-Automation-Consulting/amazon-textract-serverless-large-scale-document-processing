import boto3
import sys
import re
import json
import time
import csv


def get_kv_map():

    # with open(file_name, 'rb') as file:
    #     img_test = file.read()
    #     bytes_test = bytearray(img_test)
    #     print('Image loaded', file_name)

    # # process using image bytes
    
    # client = boto3.client('textract')

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
)

    # Needs to be replaced with https://docs.aws.amazon.com/textract/latest/dg/api-async.html SQS Queue Monitoring
    time.sleep(60)

    response = textract.get_document_analysis(
        JobId=data['JobId']
    )


    # response = client.analyze_document(Document={'Bytes': 
    # bytes_test}, FeatureTypes=['FORMS'])

    # Get the text blocks
    blocks=response['Blocks']
    

    # get key and value maps
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    return key_map, value_map, block_map, blocks


def get_kv_relationship(key_map, value_map, block_map):
    kvs = {}
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs[key] = val
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '    

                                
    return text


def print_kvs(kvs):
    for key, value in kvs.items():
        print(key, ":", value)


def search_value(kvs, search_key):
    for key, value in kvs.items():
        if re.search(search_key, key, re.IGNORECASE):
            return value

def main():

    key_map, value_map, block_map, blocks = get_kv_map()

    # Get Key Value relationship
    kvs = get_kv_relationship(key_map, value_map, block_map)
    print("\n\n== FOUND KEY : VALUE pairs ===\n")
    print_kvs(kvs)


    # now we will open a file for writing
    data_file = open('data_file.csv', 'w')
    
    # create the csv writer object
    csv_writer = csv.writer(data_file)

    # Writing headers of CSV file
    header = kvs.keys()
    csv_writer.writerow(header)
    
    # Writing data of CSV file
    csv_writer.writerow(kvs.values())
    
    data_file.close()





    # # Start searching a key value
    # while input('\n Do you want to search a value for a key? (enter "n" for exit) ') != 'n':
    #     search_key = input('\n Enter a search key:')
    #     print('The value is:', search_value(kvs, search_key))

if __name__ == "__main__":
    main()