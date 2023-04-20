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

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, element_at, split
from awsglue.context import GlueContext
from awsglue import DynamicFrame
from awsglue.job import Job
import json
# from avro.schema import make_avsc_object
import boto3
import ast
import time

"""Initialise required spark conntext/logging variables"""

sc = SparkContext().getOrCreate()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Declare variables/initialise clients
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'personalize_data_bucket', 'personalize_bucket_name',
                                     'personalize_video_dataset_group', 'personalize_news_dataset_group', 'personalize_import_role'])
personalize = boto3.client('personalize')
s3 = boto3.client('s3')

# Read the interactions data
df = glueContext.create_dynamic_frame.from_options("s3", {'paths': [f"s3://{args['personalize_data_bucket']}/"],
                                                          'recurse': True, 'groupFiles': 'inPartition',
                                                          'groupSize': '1048576'}, format="json")


def extract_personalize_dataset(df, dataset_type):
    '''
    Transform the user behaviour data to capture video/news events
    :param df: raw user interactions data
    :return df_interactions: Transformed interactions to add to Amazon personalize
    '''
    df_interactions = df.toDF()

    # Filter out the correct events
    df_interactions = df_interactions.filter((df_interactions.event_type == "screen_view"))

    if dataset_type == "video":
        df_interactions = df_interactions.filter((df_interactions.attributes.screen_name == "video-player"))
        
        df_interactions = df_interactions.withColumn("URL", df_interactions.attributes.screen_class)
        # Capture the correct column names required by personalize
        # The unique video (content) ID is present in the URL
        df_interactions = df_interactions.select((df_interactions.attributes.personalization_id).alias("USER_ID"), \
                                                 element_at(split(df_interactions.URL, '/'), -4).alias("ITEM_ID"), \
                                                 (df_interactions.event_timestamp).alias("TIMESTAMP"))

    if dataset_type == "news":
        df_interactions = df_interactions.filter((df_interactions.attributes.screen_name == "news-detail"))
        
        df_interactions = df_interactions.withColumn("STUB", df_interactions.attributes.screen_class)
        # Capture the correct column names required by personalize
        # The unique news (content) ID is the STUB
        df_interactions = df_interactions.select((df_interactions.attributes.personalization_id).alias("USER_ID"), \
                                                 (df_interactions.STUB).alias("ITEM_ID"), \
                                                 (df_interactions.event_timestamp).alias("TIMESTAMP"))

    # Duplicate the dataframe 8 times over
    for i in range(3):
        # New timestamp to ensure unique value of duplicate record
        df_interactions_unique = df_interactions.withColumn("TIMESTAMP", (df_interactions.TIMESTAMP + i + 1))
        df_interactions = df_interactions.union(df_interactions_unique)

    logger.info(df_interactions.show(20))
    print(df_interactions.count())

    return df_interactions


def write_to_S3(df_interactions, dataset_type):
    '''
    Create the initial load of the interactions dataset in the S3 personalize bucket
    :param df_interactions: Transformed interactions to add to Amazon personalize
    :return response
    '''

    return df_interactions.repartition(1).write.option("header", True).csv(
        f"s3a://{args['personalize_bucket_name']}/{dataset_type}/interactions", mode="overwrite")


def push_to_personalize(dataset_type, dataset_group):
    '''
    Create the initial load of the interactions dataset for video/news in Amazon personalize
    :return import job description
    '''

    # Create the interactions dataset schema
    schema_name = f'fanapp-{dataset_type}-interactions-schema'
    schema_exist,schema_arn = check_schema(schema_name)
    if schema_exist == False: 
        createSchemaResponse = personalize.create_schema(
            name=schema_name,
            schema=json.dumps(personalize_schema())
        )
        schema_arn = createSchemaResponse['schemaArn']
    logger.info(dataset_type + ' schema arn:' + schema_arn)

    # Create the interactions dataset
    createDatasetResponse = personalize.create_dataset(
        name=f'fanapp-{dataset_type}-interactions-dataset',
        schemaArn=schema_arn,
        datasetGroupArn=dataset_group,
        datasetType='Interactions'
    )
    dataset_arn = createDatasetResponse['datasetArn']
    logger.info(dataset_type + ' dataset arn: ' + dataset_arn)

    # Allow creation of the dataset
    time.sleep(120)

    # Create the interactions dataset import job
    createDatasetImportResponse = personalize.create_dataset_import_job(
        jobName=f'interactions-initial-import-{dataset_type}',
        datasetArn=dataset_arn,
        dataSource={'dataLocation': f"s3://{args['personalize_bucket_name']}/{dataset_type}/interactions/"},
        roleArn=args['personalize_import_role']
    )
    dsij_arn = createDatasetImportResponse['datasetImportJobArn']
    logger.info(dataset_type + ' dataset import job arn: ' + dsij_arn)

    return personalize.describe_dataset_import_job(
        datasetImportJobArn=dsij_arn)['datasetImportJob']


def get_filenames():
    '''
    Get the interaction dataset file name
    :param df_interactions: Transformed interactions to add to Amazon personalize
    :return response
    '''
    filenames = []
    result = s3.list_objects_v2(Bucket=args['personalize_bucket_name'], Prefix="interactions")
    for item in result['Contents']:
        files = item['Key']
        filenames.append(files)  # optional if you have more filefolders to got through.
    print(filenames)
    return filenames[0]

def check_schema(schema_name):
    '''
    schema_name: the name of the schema we want to test 
    :return boolean saying if the schema already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_schemas()
    for schema in response["schemas"]:
        if schema["name"] == schema_name:
            return True, schema["schemaArn"]
    return(False,"")

def personalize_schema():
    '''
    Return the personalize interactions dataset schema
    :return dict interactions_schema
    '''

    return {
        "type": "record",
        "name": "Interactions",
        "namespace": "com.amazonaws.personalize.schema",
        "fields": [
            {
                "name": "USER_ID",
                "type": "string"
            },
            {
                "name": "ITEM_ID",
                "type": "string"
            },
            {
                "name": "TIMESTAMP",
                "type": "long"
            }
        ],
        "version": "1.0"
    }

df_interactions = extract_personalize_dataset(df, "video")
response = write_to_S3(df_interactions, "video")
logger.info("Writing the videos personalize dataset to S3" + str(response))
push_to_personalize("video", args['personalize_video_dataset_group'])

df_interactions = extract_personalize_dataset(df, "news")
response = write_to_S3(df_interactions, "news")
logger.info("Writing the news personalize dataset to S3" + str(response))
push_to_personalize("news", args['personalize_news_dataset_group'])
