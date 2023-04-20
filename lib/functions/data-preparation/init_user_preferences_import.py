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

# Import needed modules
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import pandas as pd
import time
import logging

# Environment variables
p13n = os.environ['P13N']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]
account_id = os.environ["ACCOUNT_ID"]
s3_bucket = os.environ["S3_BUCKET_NAME"]
role_import_arn = os.environ["ROLE_IMPORT"]
dataset_video_group_arn = os.environ["DATASET_VIDEO_GROUP_ARN"]
dataset_news_group_arn = os.environ["DATASET_NEWS_GROUP_ARN"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
personalize = boto3.client('personalize')
iam = boto3.client("iam")


# Import resources needed
user_table = dynamodb.Table("fan-app-profiles-" + stage)
s3_object_name = "user-meta.csv"


def extract_pref(pref):
    '''
    Extract preferences information for one user 
    :pref: the preferences given by the user 
    :return 3 strings for drivers, cars, and circuits preferences  
    '''
    # pref = json.loads(pref)["answers"]["L"]
    pref = pref["answers"]
    # extract first driver placed at the first position
    drivers = pref[0]["values"]
    d = []
    for item in drivers:
        driver = item
        d.append(driver)
    drivers = "|".join(d)

    # same for car
    cars = pref[1]["values"]
    c = []
    for item in cars:
        car = item
        c.append(car)
    cars = "|".join(c)

    # and for circuits
    circuits = pref[2]["values"]
    c = []
    for item in circuits:
        circuit = item
        c.append(circuit)
    circuits = "|".join(c)

    return (drivers, cars, circuits)


def extract(user_ids, cars, circuits, drivers, data):
    '''
    Store in 4 separate lists the data about each users 
    :user_ids,cars,circuits,drivers: the previous data already extracted 
    :data: the new data to be processed and separated into 4 lists 
    :return 4 lists with the user id and the 3 preferences 
    '''
    for user in data:
        perso_id = user["personalizationId"]
        if perso_id not in user_ids:
            pref = user["answers"]
            dr, ca, ci = extract_pref(pref)
            user_ids.append(str(perso_id))
            cars.append(str(ca))
            circuits.append(str(ci))
            drivers.append(str(dr))
    return (user_ids, cars, circuits, drivers)


def users_to_s3(user_table, s3_bucket, s3_object_name):
    '''
    Put the user preferences data to a S3 bucket 
    :user_table: the dynamoDB table where the user data are
    :s3_bucket: the destination bucket
    :s3_object_name: the name of the file where we will store the users data
    :return response  
    '''
    logger.info("Exporting all data to the S3 bucket for Personalize")
    user_ids = []
    cars = []
    circuits = []
    drivers = []

    response = user_table.scan(
        FilterExpression=Attr('sk').eq("fanApp#onboarding#"))
    data = response["Items"]
    user_ids, cars, circuits, drivers = extract(
        user_ids, cars, circuits, drivers, data)

    # maximum data set limit is 1MB
    while 'LastEvaluatedKey' in response:
        response = user_table.scan(
            Select='ALL_ATTRIBUTES',
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression=Attr('sk').eq("fanApp#onboarding#"))
        data = response['Items']
        user_ids, cars, circuits, drivers = extract(
            user_ids, cars, circuits, drivers, data)

    user_data = pd.DataFrame()
    user_data["USER_ID"] = user_ids
    user_data["FAV_DRIVERS"] = drivers
    user_data["FAV_CARS"] = cars
    user_data["FAV_CIRCUITS"] = circuits

    response = s3.Bucket(s3_bucket).Object(
        s3_object_name).put(Body=user_data.to_csv(index=False))
    return (response)

def to_personalize(s3_bucket, s3_object_name, role_import_arn, dataset_type, dataset_group_arn):
    '''
    put all the user data to a personalize dataset 
    :s3_bucket: the s3 bucket where to fetch data
    :s3_object_name: the name of the object where the data are stored  
    :dataset_group_arn: the arn of the dataset group where to create the dataset
    :role_import_arn:the role to import data to personalize
    :dataset_type: news or videos 
    :return response
    '''
    logger.info("Creating the user schema for Personalize")
    users_schema = {
        "type": "record",
        "name": "Users",
        "namespace": "com.amazonaws.personalize.schema",
        "fields": [
            {
                "name": "USER_ID",
                "type": "string",
            },
            {
                "name": "FAV_DRIVERS",
                "type": [
                        "null",
                        "string"
                ],
                "categorical": True
            },
            {
                "name": "FAV_CARS",
                "type": [
                        "null",
                        "string"
                ],
            },
            {
                "name": "FAV_CIRCUITS",
                "type": [
                        "null",
                        "string"
                ],
            },

        ],
        "version": "1.0"
    }

    # First create schema
    schema_name = f'fanapp-{dataset_type}-users-schema'
    schema_exist, users_schema_arn = check_schema(schema_name)
    if schema_exist == False:
        create_schema_response = personalize.create_schema(
            name=schema_name,
            schema=json.dumps(users_schema),
        )
        # check if works
        users_schema_arn = create_schema_response['schemaArn']
        print(json.dumps(create_schema_response, indent=2))

    # then create the dataset
    dataset_name = f'fanapp-{dataset_type}-users-schema-' + stage
    type_dataset = "USERS"
    dataset_exist, users_dataset_arn = check_dataset(
        dataset_group_arn, type_dataset)
    if dataset_exist == False:
        logger.info("Creating the Dataset for personalize")
        create_dataset_response = personalize.create_dataset(
            name=dataset_name,
            datasetType="USERS",
            datasetGroupArn=dataset_group_arn,
            schemaArn=users_schema_arn
        )
        users_dataset_arn = create_dataset_response['datasetArn']
        # test if work
        logger.info(json.dumps(create_dataset_response, indent=2))

    # Wait until the dataset is created before imported it
    status = ""
    while status != "ACTIVE" and status != "CREATE FAILED":
        describe_dataset_group_response = personalize.describe_dataset(
            datasetArn=users_dataset_arn
        )
        status = describe_dataset_group_response["dataset"]["status"]
        logger.info("DatasetGroup: {}".format(status))

        if status == "ACTIVE" or status == "CREATE FAILED":
            break
        # wait 1 minute to check again
        time.sleep(60)

    # Finally import all to the dataset
    import_job_name = f'fanapp-{dataset_type}-user-import-bulk-' + stage
    import_job_exist, users_dataset_import_job_arn = check_import_job(
        users_dataset_arn, import_job_name)
    if import_job_exist == False:
        logger.info("Initial import of the User data in the dataset")
        create_dataset_import_job_response = personalize.create_dataset_import_job(
            jobName=import_job_name,
            datasetArn=users_dataset_arn,
            dataSource={
                "dataLocation": "s3://{}/{}".format(s3_bucket, s3_object_name)
            },
            roleArn=role_import_arn
        )
        users_dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']

        response = json.dumps(create_dataset_import_job_response, indent=2)
        return (response)
    return ()


def check_schema(schema_name):
    '''
    schema_name: the name of the schema we want to test 
    :return boolean saying if the schema already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_schemas(maxResults=100)
    for schema in response["schemas"]:
        if schema["name"] == schema_name:
            return True, schema["schemaArn"]
    return (False, "")


def check_dataset(dataset_group_arn, type_dataset):
    '''
    dataset_group_arn: the arn of the dataset group, where the dataset should be built 
    type_dataset: the type of dataset we want to build (Users, items or interactions)
    :return boolean saying if the dataset already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_datasets(
        datasetGroupArn=dataset_group_arn, maxResults=100)
    for dataset in response["datasets"]:
        if dataset["datasetType"] == type_dataset:
            return True, dataset["datasetArn"]
    return (False, "")


def check_import_job(dataset_arn, import_job_name):
    '''
    dataset_arn: the arn of the dataset, where the dataset should be built 
    import_job_name: the name of the import job we want to create
    :return boolean saying if this import job already exist or not, and if exist, return also the ARN of it 
    '''
    response = personalize.list_dataset_import_jobs(
        datasetArn=dataset_arn, maxResults=100)
    for job in response['datasetImportJobs']:
        if job["jobName"] == import_job_name:
            return True, job["datasetImportJobArn"]
    return (False, "")


def handler(event, context):
    '''
    Combine all the steps together from taking the inital data from the user table to putting them to the personalize dataset for users 
    :return responses for each step 
    '''
    # first put processed data to the intermediate Table
    response1 = users_to_s3(user_table, s3_bucket, s3_object_name)
    response2 = to_personalize(
        s3_bucket, s3_object_name, role_import_arn, "videos", dataset_video_group_arn)
    response3 = to_personalize(
        s3_bucket, s3_object_name, role_import_arn, "news", dataset_news_group_arn)

    return (response1, response2, response3)
