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
from datetime import date
import http.client
from fan_app_thron_utils import authenticate_request,get_thron_config,get_content_details,write_to_ddb,get_thron_public_folder

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
thron_cnf_secrets = get_thron_config()
client_id = thron_cnf_secrets['clientId']
thronParsedUri = urlparse(os.environ['THRON_HOST'])
thronExportContentUrlPath = '/api/xcontents/resources/sync/export/' + client_id


def handler(event, context):
    '''
    Lambda handler function
    :return:
    '''

    logger.info(f'Authenticating the thron app')
    x_token_id = authenticate_request()

    maxItems = event.get('maxItems',0)

    logger.info(f'Initial extraction thron items')
    items =  create_mappings(x_token_id,maxItems)

    """Initialise datetime variables"""
    today = date.today()

    logger.info(f'Writing the mappings to DynamoDB')
    write_to_ddb(items,today)


def create_mappings(x_token_id, maxItems = 0):
    '''
    Extracts the initial upload
    :param str x_token_id: The id of the app in thron
    :return: dict items: The list of video content from Thron
    '''

    conn = http.client.HTTPSConnection(thronParsedUri.hostname)
    payload = json.dumps({
        "criteria": {
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
    conn.request("POST", thronExportContentUrlPath, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))["items"]
