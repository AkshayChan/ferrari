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

"""Update the campaign (endpoint) with new solution version (model)"""
import os
import logging
import boto3


"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)

personalize = boto3.client('personalize')
personalize_runtime = boto3.client('personalize-runtime')
ssm = boto3.client('ssm')

stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]
VIDEO_CAMPAIGN_ARN = ssm.get_parameter(
    Name=f"/fan-appvideo/{stage}/Similar_items/campaignArn")["Parameter"]["Value"]
NEWS_CAMPAIGN_ARN = ssm.get_parameter(
    Name=f"/fan-appnews/{stage}/Similar_items/campaignArn")["Parameter"]["Value"]


def update_endpoint(campaign_arn, sims_solution_version_arn):
    sims_create_campaign_response = personalize.update_campaign(
        campaignArn=campaign_arn,
        solutionVersionArn=sims_solution_version_arn,
        minProvisionedTPS=1
    )
    sims_campaign_arn = sims_create_campaign_response['campaignArn']
    logger.info(f"Updated campaign: ${sims_campaign_arn}")
    return sims_campaign_arn


def get_current_solution_version(campaign_arn):
    current_campaign = personalize.describe_campaign(
        campaignArn=campaign_arn
    )['campaign']

    return current_campaign['solutionVersionArn']


def is_new_model_better(new_solution_version, old_solution_version):
    old_metrics = personalize.get_solution_metrics(
        solutionVersionArn=old_solution_version
    )["metrics"]
    old_ndcg_5 = old_metrics["normalized_discounted_cumulative_gain_at_5"]
    old_precision_5 = old_metrics["precision_at_5"]
    new_metrics = personalize.get_solution_metrics(
        solutionVersionArn=new_solution_version
    )["metrics"]
    new_ndcg_5 = new_metrics["normalized_discounted_cumulative_gain_at_5"]
    new_precision_5 = new_metrics["precision_at_5"]
    if new_ndcg_5 >= old_ndcg_5 and new_precision_5 >= old_precision_5:
        print("New model is not worse that the one in production.")
        return True
    else:
        print("New model is worse that the one in production.")
        return False


def handler(event, context):
    video_current_solution_version = get_current_solution_version(
        VIDEO_CAMPAIGN_ARN)
    update_video_model = is_new_model_better(
        event['Payload'][0], video_current_solution_version)
    if update_video_model == True:
        video_campaign_arn = update_endpoint(
            VIDEO_CAMPAIGN_ARN, event['Payload'][0])
    else:
        video_campaign_arn = None

    news_current_solution_version = get_current_solution_version(
        NEWS_CAMPAIGN_ARN)
    update_news_model = is_new_model_better(
        event['Payload'][1], news_current_solution_version)
    if update_news_model == True:
        news_campaign_arn = update_endpoint(
            NEWS_CAMPAIGN_ARN, event['Payload'][1])
    else:
        news_campaign_arn = None

    campaign_version_arn = [video_campaign_arn, news_campaign_arn]
    return campaign_version_arn
