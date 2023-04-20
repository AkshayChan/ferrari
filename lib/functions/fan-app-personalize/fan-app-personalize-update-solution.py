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

"""Create new solution version for news/video"""
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

VIDEO_SOLUTION_ARN = ssm.get_parameter(
    Name=f"/fan-appvideo/{stage}/Similar_items/solutionArn")["Parameter"]["Value"]
NEWS_SOLUTION_ARN = ssm.get_parameter(
    Name=f"/fan-appnews/{stage}/Similar_items/solutionArn")["Parameter"]["Value"]


def create_sims_solution_version(sims_solution_arn):
    sims_create_solution_version_response = personalize.create_solution_version(
        solutionArn=sims_solution_arn
    )

    sims_solution_version_arn = sims_create_solution_version_response['solutionVersionArn']
    logger.info(f"Solution version created: {sims_solution_version_arn}")

    return sims_solution_version_arn


def handler(event, context):
    video_solution_version_arn = create_sims_solution_version(
        VIDEO_SOLUTION_ARN)
    news_solution_version_arn = create_sims_solution_version(NEWS_SOLUTION_ARN)

    solution_version_arn = [
        video_solution_version_arn, news_solution_version_arn]
    return solution_version_arn
