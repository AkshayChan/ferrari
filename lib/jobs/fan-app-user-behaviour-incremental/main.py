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
import boto3
from datetime import datetime, timedelta

"""Initialise required spark conntext/logging variables"""

sc = SparkContext().getOrCreate()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# Get the year, month and yesterday for reading 24hrs interaction data
yesterday = datetime.now() - timedelta(1)
year = yesterday.strftime("%Y")
month = yesterday.strftime("%m")
day = yesterday.strftime("%d")

# Declare variables/initialise clients
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'personalize_data_bucket', 'personalize_bucket_name',
                                     'personalize_video_dataset_group', 'personalize_news_dataset_group', 'personalize_import_role'])
personalize = boto3.client('personalize')
s3 = boto3.client('s3')

# Read the interactions data for yesterday
df = glueContext.create_dynamic_frame.from_options("s3", {'paths': [f"s3://{args['personalize_data_bucket']}/{year}/{month}/{day}"],
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
    df_interactions = df_interactions.filter(
        (df_interactions.event_type == "screen_view"))

    if dataset_type == "video":
        df_interactions = df_interactions.filter(
            (df_interactions.attributes.screen_name == "video-player"))
        df_interactions = df_interactions.withColumn(
            "URL", df_interactions.attributes.screen_class)
        # Capture the correct column names required by personalize
        # The unique video (content) ID is present in the URL
        df_interactions = df_interactions.select((df_interactions.attributes.personalization_id).alias("USER_ID"),
                                                 element_at(
                                                     split(df_interactions.URL, '/'), -4).alias("ITEM_ID"),
                                                 (df_interactions.event_timestamp).alias("TIMESTAMP"))

    if dataset_type == "news":
        df_interactions = df_interactions.filter(
            (df_interactions.attributes.screen_name == "news-detail"))
        df_interactions = df_interactions.withColumn(
            "STUB", df_interactions.attributes.screen_class)
        # Capture the correct column names required by personalize
        # The unique news (content) ID is the STUB
        df_interactions = df_interactions.select((df_interactions.attributes.personalization_id).alias("USER_ID"),
                                                 (df_interactions.STUB).alias(
                                                     "ITEM_ID"),
                                                 (df_interactions.event_timestamp).alias("TIMESTAMP"))

    logger.info(df_interactions.show(20))
    print(df_interactions.count())

    return df_interactions


def write_to_S3(df_interactions, dataset_type):
    '''
    Create the incremental load of the interactions dataset in the S3 personalize bucket
    :param df_interactions: Transformed interactions to add to Amazon personalize
    :return response
    '''

    return df_interactions.repartition(1).write.option("header", True).csv(
        f"s3a://{args['personalize_bucket_name']}/{dataset_type}/interactions/{year}-{month}-{day}", mode="overwrite")


def push_to_personalize(dataset_type, dataset_group):
    '''
    Create the incremental load of the interactions dataset for video/news in Amazon personalize
    :return import job description
    '''

    # Assumes that the dataset exists - parse out the interactions dataset ARN
    ListDatasetsResponse = personalize.list_datasets(
        datasetGroupArn=dataset_group
    )
    datasets_list = ListDatasetsResponse['datasets']
    interactions_dataset_arn = [
        x for x in datasets_list if "interactions" in x["name"]][0]["datasetArn"]

    # Create the incremental interactions dataset import job
    createDatasetImportResponse = personalize.create_dataset_import_job(
        jobName=f'interactions-incremental-import-{dataset_type}-{year}-{month}-{day}',
        datasetArn=interactions_dataset_arn,
        dataSource={
            'dataLocation': f"s3://{args['personalize_bucket_name']}/{dataset_type}/interactions/{year}-{month}-{day}/"},
        roleArn=args['personalize_import_role'],
        importMode='INCREMENTAL'
    )
    dsij_arn = createDatasetImportResponse['datasetImportJobArn']
    logger.info(dataset_type +
                ' dataset incremental import job arn: ' + dsij_arn)

    return personalize.describe_dataset_import_job(
        datasetImportJobArn=dsij_arn)['datasetImportJob']


def put_events_personalize(spark_df, dataset_type):
    personalize_events = boto3.client(service_name='personalize-events')
    ssm = boto3.client('ssm')

    tracking_id = ssm.get_parameter(
        Name=f"/fan-app{dataset_type}/Event_tracker/tracking_id")["Parameter"]["Value"]

    df = spark_df.toPandas()
    for user in df.USER_ID.unique():
        df_user = df[df["USER_ID"] == user]
        eventslist = []
        for index, row in df_user.iterrows():
            event = {
                'sentAt': datetime.fromtimestamp(float(int(row["TIMESTAMP"])/1000)),
                'eventType': 'view',
                'itemId': row["ITEM_ID"]
            }
            eventslist.append(event)
            if (len(eventslist) == 10):
                personalize_events.put_events(
                    trackingId=tracking_id,
                    userId=user,
                    sessionId=user,
                    eventList=eventslist
                )
                eventslist = []
        if (len(eventslist) > 0):
            personalize_events.put_events(
                trackingId=tracking_id,
                userId=user,
                sessionId=user,
                eventList=eventslist
            )


df_interactions = extract_personalize_dataset(df, "video")
response = write_to_S3(df_interactions, "video")
logger.info(
    "Writing the videos personalize dataset for yesterday to S3" + str(response))
if df_interactions.count() >= 1000:
    push_to_personalize("video", args['personalize_video_dataset_group'])
else:
    put_events_personalize(df_interactions, "video")

df_interactions = extract_personalize_dataset(df, "news")
response = write_to_S3(df_interactions, "news")
logger.info(
    "Writing the news personalize dataset for yesterday to S3" + str(response))
if df_interactions.count() >= 1000:
    push_to_personalize("news", args['personalize_news_dataset_group'])
else:
    put_events_personalize(df_interactions, "news")
