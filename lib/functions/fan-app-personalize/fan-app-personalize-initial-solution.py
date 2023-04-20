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

"""Create the initial solution version for news/video dataset groups"""
import os
import logging
import boto3
from botocore.exceptions import ClientError

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)

video_dataset_group_arn = os.environ['VIDEO_DATASET_GROUP']
news_dataset_group_arn = os.environ['NEWS_DATASET_GROUP']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]

personalize = boto3.client('personalize')
personalize_runtime = boto3.client('personalize-runtime')
ssm = boto3.client('ssm')


def create_solution(dataset_group_arn, dataset_type):
    similar_items_recipe_arn = "arn:aws:personalize:::recipe/aws-similar-items"
    similar_items_solution_name = f"aws-similar-items-{dataset_type}"

    try:
        sims_create_solution_response = personalize.create_solution(
            name=similar_items_solution_name,
            datasetGroupArn=dataset_group_arn,
            recipeArn=similar_items_recipe_arn
        )
        similar_items_solution_arn = sims_create_solution_response['solutionArn']
        logger.info(
            f"Solution created: {similar_items_solution_arn} for dataset type {dataset_type}")
        return similar_items_solution_arn

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            print("Solution already exists. (", e, ")")
            solutions = {s["name"]: s["solutionArn"]
                         for s in personalize.list_solutions()["solutions"]}
            print(solutions)
            similar_items_solution_arn = solutions[similar_items_solution_name]
            return similar_items_solution_arn
        else:
            print(e)


def create_solution_version(solution_arn, dataset_group_arn):
    similar_items_create_solution_version_response = personalize.create_solution_version(
        solutionArn=solution_arn
    )

    similar_items_solution_version_arn = similar_items_create_solution_version_response[
        'solutionVersionArn']
    logger.info(
        f"Solution version created: {similar_items_solution_version_arn} for solution {solution_arn}")
    return similar_items_solution_version_arn


def handler(event, context):

    video_solution_arn = create_solution(video_dataset_group_arn, "video")
    news_solution_arn = create_solution(news_dataset_group_arn, "news")

    video_solution_version_arn = create_solution_version(
        video_solution_arn, video_dataset_group_arn)
    news_solution_version_arn = create_solution_version(
        news_solution_arn, news_dataset_group_arn)

    ssm_response_video = ssm.put_parameter(
        Name=f"/fan-appvideo/{stage}/Similar_items/solutionArn",
        Description='Similar items solution ARN for video',
        Value=video_solution_arn,
        Type='String',
        Overwrite=True
    )

    ssm_response_news = ssm.put_parameter(
        Name=f"/fan-appnews/{stage}/Similar_items/solutionArn",
        Description='Similar items solution ARN for news',
        Value=news_solution_arn,
        Type='String',
        Overwrite=True
    )
    solution_version_arn = [
        video_solution_version_arn, news_solution_version_arn]
    return solution_version_arn
