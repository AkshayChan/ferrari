# © 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved. This
# AWS Content is provided subject to the terms of the AWS Customer Agreement
# available at http://aws.amazon.com/agreement or other written agreement between
# Customer and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL
# or both.

# Any code, applications, scripts, templates, proofs of concept, documentation
# and other items provided by AWS under this SOW are "AWS Content," as defined
# in the Agreement, and are provided for illustration purposes only. All such
# AWS Content is provided solely at the option of AWS, and is subject to the
# terms of the Addendum and the Agreement. Customer is solely responsible for
# using, deploying, testing, and supporting any code and applications provided
# by AWS under this SOW.

import json
import boto3
import sys
from dynamodb_json import json_util as json2
import csv
import pandas as pd
from pandas.io.json import json_normalize
from io import StringIO
import os
import time
from time import sleep
from datetime import datetime


client = boto3.client('dynamodb')
personalize = boto3.client('personalize')
ssm = boto3.client('ssm')

p13n =  os.environ['P13N']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]
video_group_arn = os.environ["VIDEO_GROUP_ARN"]
news_group_arn = os.environ["NEWS_GROUP_ARN"]
s3_name = os.environ["CONTENT_BUCKET"]
role_import_arn = os.environ["ROLE_IMPORT"]
ddb_table = os.environ['CONTENT_TABLE']

news_schema = {
        "type": "record",
        "name": "Items",
        "namespace": "com.amazonaws.personalize.schema",
        "fields": [
            {
                "name": "ITEM_ID",
                "type": "string"
            },
            # {
            #     "name": "CONTENT_URL",
            #     "type": "string",
            # },
            {
                "name": "CONTENT_TYPE",
                "type": "string",
            },
            {
                "name": "CHANNEL",
                "type": "string",
            },
            {
                "name":"PLACE",
                "type": "string",
            },
            # {
            #     "name":"THUMB_DESC",
            #     "type":"string",
            # },
            {
                "name":"THUMB",
                "type":"string"
            },
            {
                "name":"NAME_TITLE",
                "type":"string"
            },
            {
                "name": "TAGS",
                "type": [
                    "null",
                    "string"
                ],
                "categorical": True,
            },
            
        ],
        "version": "1.0"
    }


video_schema = {
        "type": "record",
        "name": "Items",
        "namespace": "com.amazonaws.personalize.schema",
        "fields": [
            {
                "name": "ITEM_ID",
                "type": "string"
            },
            # {
            #     "name": "CONTENT_URL",
            #     "type": "string",
            # },
            {
                "name": "CONTENT_TYPE",
                "type": "string",
            },
            {
                "name":"DESCRIPTION",
                "type":"string",
            },
            {
                "name":"DURATION",
                "type":"string",
            },
            {
                "name":"THUMB",
                "type":"string"
            },
            {
                "name":"NAME_TITLE",
                "type":"string"
            },
            {
                "name": "TAGS",
                "type": [
                    "null",
                    "string"
                ],
                "categorical": True,
            },
            
        ],
        "version": "1.0"
    }



def check_dataset(dataset_group_arn,type_dataset):
    '''
    dataset_group_arn: the arn of the dataset group, where the dataset should be built 
    type_dataset: the type of dataset we want to build (Users, items or interactions)
    :return boolean saying if the dataset already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_datasets(datasetGroupArn=dataset_group_arn,maxResults=100)
    for dataset in response["datasets"]:
        if dataset["datasetType"] == type_dataset:
            return True, dataset["datasetArn"]
    return(False,"")


def check_schema(schema_name):
    '''
    schema_name: the name of the schema we want to test 
    :return boolean saying if the schema already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_schemas(maxResults=100)
    for schema in response["schemas"]:
        if schema["name"] == schema_name:
            return True, schema["schemaArn"]
    return(False,"")


def create_personalize_dataset(final_data, bucket, name, dataset_group_arn, content_schema):
    # S3 part----- VIDEO
    # creating a pandas dataframe based on list object and flattening json object into table format
    df = json_normalize(final_data) 
    
    # writing the dataframe to csv and uploading on S3 bucket
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, f'{name}-content-meta.csv').put(Body=csv_buffer.getvalue())
    

    # Personalize part------
    # Create schema for Personalize dataset
    schema_name = f"{p13n}{name}-dataset-content-schema-{stage}"
    schema_exist, content_schema_arn = check_schema(schema_name)
    # just for testing different schemas
    # if schema_exist:
    #     personalize.delete_schema(schemaArn=content_schema_arn)
    #     create__dataset_schema = personalize.create_schema(
    #         name = schema_name,
    #         schema = json.dumps(content_schema),
    #     )
        
    #     content_schema_arn = create__dataset_schema['schemaArn']
    if schema_exist == False: 
        create__dataset_schema = personalize.create_schema(
            name = schema_name,
            schema = json.dumps(content_schema),
        )
        
        content_schema_arn = create__dataset_schema['schemaArn']
    
    # Creating Personalize dataset 
    dataset_name = f"{p13n}{name}-content-dataset-{stage}"
    type_dataset = "ITEMS"
    dataset_exist, content_dataset_arn = check_dataset(dataset_group_arn, type_dataset)

    if dataset_exist == False: 
        create_content_dataset = personalize.create_dataset(
            name = dataset_name,
            datasetType = "Items",
            datasetGroupArn = dataset_group_arn,
            schemaArn = content_schema_arn
        )

        content_dataset_arn = create_content_dataset['datasetArn']
    # #test if work
    # print(json.dumps(create_content_dataset, indent=2))
    
    # Populating Personalize dataset by reading file from S3
    
    s3_bucket = bucket
    s3_object = f'{name}-content-meta.csv'

    #finally import all to the dataset
    max_time = time.time() + 15*60 # 15 mins
    while time.time() < max_time:
        describe_dataset_group_response = personalize.describe_dataset(
        datasetArn = content_dataset_arn
        )
        status = describe_dataset_group_response["dataset"]["status"]
        print("DatasetGroup: {}".format(status))
    
        if status == "ACTIVE" or status == "CREATE FAILED":
            break
        
        time.sleep(10)
    
    time_now = datetime.now().strftime("%Y%m%d%H%m")
    create_dataset_import_job_response_bulk = personalize.create_dataset_import_job(
        jobName = f"{p13n}{name}-content-import-bulk-{stage}-{time_now}",
        datasetArn = content_dataset_arn,
        dataSource = {
            "dataLocation": "s3://{}/{}".format(s3_bucket, s3_object)
            
        },
        roleArn = role_import_arn
    )

    users_dataset_import_job_arn_bulk = create_dataset_import_job_response_bulk['datasetImportJobArn']

    ssm_response = ssm.put_parameter(
        Name=f"/{p13n}/{stage}/{name}ContentDataSetArn",
        Description=f'Content Dataset ARN for {name}',
        Value=content_dataset_arn,
        Type='String',
        Overwrite=True
    )
    return content_dataset_arn

def replaceIfEmpty(fieldValue):
     if (not fieldValue.strip()):
        return "-"
     return fieldValue

def lambda_handler(event, context):
    # Referencing S3 Bucket and DDB Table to map with Lambda Function 
    bucket =  s3_name
    data = client.scan(TableName=ddb_table)

    # converting DDB json to normal json format
    data_new = json2.loads(data) 
    
    final_data_video = []
    final_data_news = []
    
    for i in data_new['Items']:
        json_obj = {}
        if (not  i["contentId"]):
            continue #just skip empty contentId (be resilient!)
        # extracting each item from the content table
        json_obj["ITEM_ID"] = i["contentId"]
        # json_obj["CONTENT_URL"] = i["contentURL"]
        json_obj["CONTENT_TYPE"] = replaceIfEmpty(i["contentType"])

        # extracting data from the channel meta-data mapping attribute
        if i["contentType"] == "news":
            json_obj["CHANNEL"] = replaceIfEmpty(i["contentMetadata"]["channel"])
            json_obj["PLACE"] = replaceIfEmpty(i["contentMetadata"]["place"])
            # json_obj["THUMB_DESC"] = i["contentMetadata"]["thumbDesc"]
        if i["contentType"] == "video":
            json_obj["DESCRIPTION"] = replaceIfEmpty(i["contentMetadata"]["description"])
            json_obj["DURATION"] = replaceIfEmpty(i["contentMetadata"]["durationMs"])
        json_obj["THUMB"] = replaceIfEmpty(i["contentMetadata"]["thumb"])
        json_obj["NAME_TITLE"] = replaceIfEmpty(i["contentMetadata"]["name_title"])
        
        # extracting tags and joining using | operator based on the items dataset requirement for Personalize
        json_obj["TAGS"] = i["contentMetadata"]["tags"] #tags can be nullable
        
        # appending extracted data to a list object
        if i["contentType"] == "video":
            final_data_video.append(json_obj)
        elif i["contentType"] == "news":
            final_data_news.append(json_obj)

    video_content_dataset_arn = create_personalize_dataset(final_data_video, bucket, "video", video_group_arn, video_schema)
    news_content_dataset_arn = create_personalize_dataset(final_data_news, bucket, "news", news_group_arn, news_schema)

    return [video_content_dataset_arn, news_content_dataset_arn]
