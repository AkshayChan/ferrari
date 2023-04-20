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

"""Create the event trackers for real time events ingestion"""

import os
import boto3
import logging

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
video_dataset_group_arn = os.environ['VIDEO_DATASET_GROUP']
news_dataset_group_arn = os.environ['NEWS_DATASET_GROUP']
stage = os.environ["STAGE"]

personalize = boto3.client('personalize')
ssm = boto3.client('ssm')

def create_event_tracker(dataset_group_arn, dataset_type):
    event_tracker_name = f"interactions-event-tracker-{dataset_type}"

    event_tracker_create_response = personalize.create_event_tracker(
        name=event_tracker_name,
        datasetGroupArn=dataset_group_arn
    )
    event_tracker_arn = event_tracker_create_response['eventTrackerArn']
    logger.info(f"Event tracker created: {event_tracker_arn} for dataset type {dataset_type}")

    ssm_response = ssm.put_parameter(
        Name=f"/fan-app{dataset_type}/Event_tracker/tracking_id",
        Description='Tracking id for events',
        Value=event_tracker_create_response['trackingId'],
        Type='String',
        Overwrite=True
    )

    return event_tracker_arn

def handler(event, context):

    # Create event trackers for both dataset groups
    video_event_tracker_arn = create_event_tracker(video_dataset_group_arn, "video")
    news_event_tracker_arn = create_event_tracker(news_dataset_group_arn, "news")

    event_tracker_arn = [video_event_tracker_arn, news_event_tracker_arn]
    return event_tracker_arn
