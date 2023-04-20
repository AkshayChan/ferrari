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

"""Process the initial content data"""
import json
import os
import boto3
import logging
import urllib.request
from urllib.parse import urlparse
import http.client
from datetime import date, datetime, timedelta
from fan_app_thron_utils import authenticate_request,get_thron_config,write_to_ddb,get_thron_public_folder


"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
content_table = os.environ['CONTENT_TABLE']
thron_cnf_secrets = get_thron_config()
client_id = thron_cnf_secrets['clientId']
thronParsedUri = urlparse(os.environ['THRON_HOST'])
thronUpdatedContentUrlPath = '/api/xcontents/resources/sync/updatedContent/' + client_id


def handler(event, context):
    '''
    Lambda handler function
    :return:
    '''
    # logger.info(f'Processing {str(len(event["Records"]))} records in this invocation')

    logger.info(f'Authenticating the thron app')
    x_token_id = authenticate_request()

    maxItems = event.get('maxItems',0)

    today = date.today()
    toDate = today.strftime("%Y-%m-%d")
    daysAgo = int(os.environ.get('DAYS_AGO',event.get('daysAgo',1))) #env variable has priority - default is 1 dayAgo
    if (daysAgo > 60):
        logger.warning("thron API doesn't accept requesting updated items older than 2 months ago. 60 dayAgo will be used")
        daysAgo = 60
    fromDate = (datetime.now() - timedelta(daysAgo)).strftime('%Y-%m-%d')

    logger.info(f'Asking Thron updated fromDate: %s toDate: %s',fromDate,toDate)
    items =  create_mappings(x_token_id, fromDate, toDate, maxItems)

    logger.info(f'Writing the mappings to DynamoDB')
    write_to_ddb(items,today)

def create_mappings(x_token_id, fromDate, toDate, maxItems = 0):
    '''
    Extracts the initial upload
    :param str x_token_id: The id of the app in thron
    :return: dict items: The list of video content from Thron
    '''

    conn = http.client.HTTPSConnection(thronParsedUri.hostname)
    payload = json.dumps({
        "criteria": {
            "fromDate": fromDate,
            "toDate": toDate,
            "contentType": [
                "IMAGE",
                "VIDEO",
                "AUDIO"
            ],
            "linkedCategoryOp": {
                "linkedCategoryIds": [
                    get_thron_public_folder()
                ],
                "cascade": True
            }
        },
        "options": {
            "returnLinkedCategories": False,
            "returnDeliveryInfo": True,
            "returnItags": True,
            "returnImetadata": False,
            "thumbDivArea": ""
        },
        "nextPage": "",
        "pageSize": maxItems
    })
    headers = {
        'X-TOKENID': x_token_id,
        'Content-Type': 'application/json'
    }
    conn.request("POST", thronUpdatedContentUrlPath, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))["items"]

