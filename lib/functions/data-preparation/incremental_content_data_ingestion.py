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

import boto3
import os
import logging
import time
from dynamodb_json import json_util as json2
from json import dumps

logger = logging.getLogger()
logger.setLevel(logging.INFO)

personalize = boto3.client('personalize')
personalize_events = boto3.client('personalize-events')
ssm = boto3.client('ssm')

p13n = os.environ['P13N']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]
video_content_dataset_arn = ssm.get_parameter(
    Name=f"/{p13n}/{stage}/videoContentDataSetArn")["Parameter"]["Value"]
news_content_dataset_arn = ssm.get_parameter(
    Name=f"/{p13n}/{stage}/newsContentDataSetArn")["Parameter"]["Value"]


def update_dataset_group(new_content_array, content_dataset_arn):
    logger.info(f"update_dataset_group for %s", content_dataset_arn)
    logger.info(new_content_array)
    # ingest data to Amazon Personalize dataset
    response = personalize_events.put_items(
        datasetArn=content_dataset_arn,
        items=new_content_array
    )
    # max 10tps per second of put_items. putting this 300ms wait to ensure we comply with it
    time.sleep(0.3)
    return response["ResponseMetadata"]


def clean_item_attribute(src):
    if not src.strip():
        return '-'
    return src.replace("'", "\'").replace("\"", "\\\"")


def lambda_handler(event, context):
    chunk_video = []
    chunk_news = []
    logger.info(event)

    for data in event["Records"]:
        if data["eventName"] == 'REMOVE':
            logger.info("skip item - nothing to do when we remove items")
            continue

        new_content = json2.loads(data["dynamodb"]["NewImage"])
        # logger.info("new content:")
        # logger.info(new_content)

        json_obj_put = {}
        json_obj_put["itemId"] = new_content["contentId"]

        properties_json = {}
        # properties_json["CONTENT_URL"] = new_content["contentURL"]
        properties_json["contentType"] = new_content["contentType"]
        properties_json["thumb"] = clean_item_attribute(
            new_content["contentMetadata"]["thumb"])
        properties_json["nameTitle"] = clean_item_attribute(
            new_content["contentMetadata"]["name_title"])
        # extracting tags and joining using | operator based on the items dataset requirement for Personalize
        if new_content["contentMetadata"]["tags"].strip():
            properties_json["tags"] = clean_item_attribute(
                new_content["contentMetadata"]["tags"])

        # extracting data from the channel meta-data mapping attribute
        if new_content["contentType"] == "news":
            properties_json["channel"] = clean_item_attribute(
                new_content["contentMetadata"]["channel"])
            properties_json["place"] = clean_item_attribute(
                new_content["contentMetadata"]["place"])
        elif new_content["contentType"] == "video":
            properties_json["description"] = clean_item_attribute(
                new_content["contentMetadata"]["description"])
            properties_json["duration"] = clean_item_attribute(
                new_content["contentMetadata"]["durationMs"])

        json_obj_put["properties"] = dumps(properties_json)

        if new_content["contentType"] == "video":
            chunk_video.append(json_obj_put)
            if len(chunk_video) == 10:
                update_dataset_group(
                    chunk_video, video_content_dataset_arn)
                chunk_video = []
        elif new_content["contentType"] == "news":
            chunk_news.append(json_obj_put)
            if len(chunk_news) == 10:
                update_dataset_group(
                    chunk_news, news_content_dataset_arn)
                chunk_news = []

    if len(chunk_video) > 0:
        update_dataset_group(
            chunk_video, video_content_dataset_arn)
    if len(chunk_news) > 0:
        update_dataset_group(
            chunk_news, news_content_dataset_arn)
