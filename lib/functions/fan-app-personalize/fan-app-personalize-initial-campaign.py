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

"""Create the initial campaign (endpoint)"""

import os
import boto3
import logging
from botocore.exceptions import ClientError

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
CAMPAIGN_NAME_VIDEO = os.environ['CAMPAIGN_NAME_VIDEO']
CAMPAIGN_NAME_NEWS = os.environ['CAMPAIGN_NAME_NEWS']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]

personalize = boto3.client('personalize')
personalize_runtime = boto3.client('personalize-runtime')
ssm = boto3.client('ssm')


def create_endpoint(solution_version_arn, campaign_name, name):
    try:
        similar_items_create_campaign_response = personalize.create_campaign(
            name=campaign_name,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=1
        )

        similar_items_campaign_arn = similar_items_create_campaign_response['campaignArn']
        logger.info(f"Created campaign: ${similar_items_campaign_arn}")
        return similar_items_campaign_arn

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            print("Campaign already exists. (", e, ")")
            existing_campaign_arn = ssm.get_parameter(
                Name=f"/fan-app{name}/{stage}/Similar_items/campaignArn")["Parameter"]["Value"]
            campaign_arn = update_endpoint(
                solution_version_arn, existing_campaign_arn)
            return campaign_arn
        else:
            print(e)


def update_endpoint(solution_version_arn, campaign_arn):
    create_campaign_response = personalize.update_campaign(
        campaignArn=campaign_arn,
        solutionVersionArn=solution_version_arn,
        minProvisionedTPS=1
    )
    campaign_arn = create_campaign_response['campaignArn']
    logger.info(f"Updated campaign: ${campaign_arn}")
    return campaign_arn


def handler(event, context):
    # Video solution version is returned first by previous Lambdas
    video_campaign_arn = create_endpoint(
        event['Payload'][0], CAMPAIGN_NAME_VIDEO, "video")
    news_campaign_arn = create_endpoint(
        event['Payload'][1], CAMPAIGN_NAME_NEWS, "news")

    ssm_response_video = ssm.put_parameter(
        Name=f"/fan-appvideo/{stage}/Similar_items/campaignArn",
        Description='Similar items campaign ARN for video',
        Value=video_campaign_arn,
        Type='String',
        Overwrite=True
    )

    ssm_response_news = ssm.put_parameter(
        Name=f"/fan-appnews/{stage}/Similar_items/campaignArn",
        Description='Similar items campaign ARN for news',
        Value=news_campaign_arn,
        Type='String',
        Overwrite=True
    )
    campaign_version_arn = [video_campaign_arn, news_campaign_arn]
    return campaign_version_arn
