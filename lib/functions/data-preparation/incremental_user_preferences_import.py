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
import os
import logging
import time
from dynamodb_json import json_util as json2

# Environment variables
p13n = os.environ['P13N']
stage = os.environ["STAGE"]
environment = os.environ["ENVIRONMENT_NAME"]
dataset_group_news_arn = os.environ["DATASET_NEWS_GROUP_ARN"]
dataset_group_videos_arn = os.environ["DATASET_VIDEO_GROUP_ARN"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

personalize = boto3.client('personalize')
personalize_events = boto3.client('personalize-events')


def extract_pref(answers):
    '''
    Extract preferences information for one user 
    :pref: the preferences given by the user 
    :return 3 strings for drivers, cars, and circuits preferences  
    '''

    # extract drivers cars and circuit in pipe-separated form
    drivers = "|".join((next((x['values'] for x in answers if 'FAVORITE_DRIVER' == x["questionId"]), None)) or "")
    cars = "|".join((next((x['values'] for x in answers if 'FAVOURITE_CAR' == x["questionId"]), None)) or "")
    circuits =  "|".join((next((x['values'] for x in answers if 'FAVOURITE_CIRCUIT' == x["questionId"]), None)) or "")

    logger.debug(f"got drivers='%s' cars='%s' circuits='%s'",drivers,cars,circuits)

    return (drivers, cars, circuits)


def check_data(event):
    '''
    Process the new user data and check what needs to be done with it 
    :event: a new data added to the table 
    :return: th id for the user we have to change, the preferences of this user and what to do with it 

    expected event as input:
    {
        "answersDate": "2022-12-23T15:37:53.853Z",
        "profileId": "profileId#anonymous#5c3a9509-9e3c-44d1-8f46-5071212e98cd",
        "personalizationId": "957aadc2-b2cf-4213-a10d-df881636a697",
        "answers": {
            "answerDate": "",
            "answers": [
            {
                "questionId": "FAVORITE_DRIVER",
                "values": ["michael_schumacher", "kimi_raikkonen"]
            },
            { "questionId": "FAVOURITE_CAR", "values": ["F2004", "F2007"] },
            { "questionId": "FAVOURITE_CIRCUIT", "values": ["suzuka"] }
            ],
            "setId": "fanApp#OnboardingQuestions#en-US#4"
        },
        "sk": "fanApp#onboarding#",
        "pk": "profileId#anonymous#5c3a9509-9e3c-44d1-8f46-5071212e98cd"
    }


    '''
    perso_id, drivers, cars, circuits = "0", "0", "0", "0"

    perso_id = event["personalizationId"]

    answers = event["answers"]["answers"]
    drivers, cars, circuits = extract_pref(answers)

    user = {
        'userId': perso_id,
        'properties': json.dumps({"favDrivers": drivers,
                                  "favCars": cars, "favCircuits": circuits})
    }
    return user


def users_to_personalize(user_chunk, dataset_arn):
    '''
    Put the new data in the personalize dataset 
    :user_chunk: array of users to put. max length 10
    :dataset_group_arn: the arn of the dataset group 
    :return: response 
    '''
    logger.info("Add to the personalize dataset %s", dataset_arn)
    personalize_events.put_users(
        datasetArn=dataset_arn,
        users=user_chunk
    )
    # max 10tps per second of put_users. putting this 300ms wait to ensure we comply with it
    time.sleep(0.3)


def get_dataset_arn(dataset_group_arn, type_dataset):
    '''
    dataset_group_arn: the arn of the dataset group, where the dataset should be built 
    type_dataset: the type of dataset we want to build (Users, items or interactions)
    :return the ARN of the dataset of type_dataset in the dataset_group
    '''
    response = personalize.list_datasets(
        datasetGroupArn=dataset_group_arn, maxResults=100)
    dataset_arn = ""
    for dataset in response["datasets"]:
        if dataset["datasetType"] == type_dataset:
            dataset_arn = dataset["datasetArn"]
    return dataset_arn


def handler(event, context):
    '''
    Combine all the steps together from taking the inital data from the user table to putting them to the personalize dataset for users 
    :return responses for each step 
    '''
    dataset_arn_news = get_dataset_arn(dataset_group_news_arn, "USERS")
    dataset_arn_videos = get_dataset_arn(dataset_group_videos_arn, "USERS")

    user_chunk = []
    events = event["Records"]
    logger.info(f"handling new user preferences from %d events", len(events))

    for i in range(len(events)):
        data = events[i]
        unique_event = json2.loads(data["dynamodb"]["NewImage"])
        user = check_data(unique_event)
        user_chunk.append(user)

        if (len(user_chunk) == 10) or (i == len(events) - 1):
            # Finally put it to the personalize dataset
            logger.info(f"sending chunk of users: %d", len(user_chunk))
            users_to_personalize(
                user_chunk, dataset_arn_news)
            users_to_personalize(
                user_chunk, dataset_arn_videos)
            user_chunk = []
