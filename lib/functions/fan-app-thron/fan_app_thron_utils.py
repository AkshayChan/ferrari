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
import http.client
from datetime import date
from urllib.parse import urlparse
import common_http_client_util as httpclientUtil

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')
content_table = os.environ['CONTENT_TABLE']
secrets_manager_client = boto3.client('secretsmanager')
thron_cnf_secrets = {}
thronAdminParsedUri = urlparse(os.environ['THRON_ADMIN_HOST'])
thronParsedUri = urlparse(os.environ['THRON_HOST'])

contentTypeToThronChannelToSearch = {
    'video' : 'WEBHD',
    'audio' : 'WEBAUDIO',
    'image' : 'WEB'
} 


def get_thron_public_folder():
    return os.environ['THRON_PUBLIC_FOLDER']

def get_thron_config():
    global thron_cnf_secrets
    if not thron_cnf_secrets:
        thron_cnf_secrets = json.loads( 
            secrets_manager_client.get_secret_value(SecretId=os.environ.get('THRON_CONFIG_SECRET_ARN'))['SecretString'] 
        )
    return thron_cnf_secrets

def authenticate_request():
    '''
    Authenticates the app to get exported/updated content
    :param str app_id: The id of the app in thron
    :param str app_key: The key use for thron authentications
    :return: str appUserTokenId: Thr authentication token from Thron server
    '''
    thron_cnf_secrets = get_thron_config()
    loginUrl = thronAdminParsedUri.scheme +'://'+thronAdminParsedUri.hostname + '/api/xadmin/resources/apps/loginApp/' + thron_cnf_secrets['clientId']

    data = {
        'appId': thron_cnf_secrets['appId'],
        'appKey': thron_cnf_secrets['appKey']
    }

    data = urllib.parse.urlencode(data).encode()

    req = urllib.request.Request(
        url=loginUrl,
        method='POST',
        data=data
    )

    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    response = urllib.request.urlopen(req).read()
    return json.loads(response)['appUserTokenId']

def get_content_details(contentId):
    '''
    returns content details content object from thron
    :param contentId: The id of the content
    :return: str content: Details of the contentId
    '''
    thron_cnf_secrets = get_thron_config()
    clientId=thron_cnf_secrets['clientId']
    pKey=thron_cnf_secrets['pKey']
    thronContentDetailUrlPath = f'/api/xcontents/resources/delivery/getContentDetail?clientId={clientId}&xcontentId={contentId}&templateId=CE1&pkey={pKey}'
    #httpclientUtil.initHttpClientDebugLevel(1)
    conn = http.client.HTTPSConnection(thronParsedUri.hostname)
    conn.request('GET',thronContentDetailUrlPath)
    res = conn.getresponse()
    data = res.read()
    if res.status == 418:
        logger.warning(f"Problems getting content details {data}")
        return None
    content_detail = {
        'contentType': '',
        'contentUrl' : None,
        'thumbUrl' : '',
        'durationMs' : 0,
        'name_title' : '',
        'description': '',
        'creationDate': None,
        'lastUpdate' : None
    }
    content_details_from_thron = json.loads(data.decode("utf-8"))["content"]
    content_detail['contentType'] = content_details_from_thron['contentType'].lower()
    thronChannelTypeToMatch = contentTypeToThronChannelToSearch[content_detail['contentType']]
    for deliveryInfo in content_details_from_thron["deliveryInfo"]:
        if thronChannelTypeToMatch not in deliveryInfo["channelType"]:
            continue
        if 'contentUrl' not in deliveryInfo:
            continue
        content_detail['contentUrl'] = deliveryInfo['contentUrl']
        content_detail['thumbUrl'] = ( next((x for x in deliveryInfo["thumbsUrl"] if '720x0' in x ), None)) or  deliveryInfo['defaultThumbUrl']
        content_detail['durationMs'] = ( next((x['value'] for x in deliveryInfo["sysMetadata"] if 'Durationms' in x['name']), None)) or 0
    localeEN = ( next((x for x in content_details_from_thron["locales"] if 'EN' in x['locale'] ), None)) 
    if (localeEN) :
       content_detail['name_title'] = localeEN['name']
       content_detail['description'] = localeEN['description']
    content_detail['creationDate'] = content_details_from_thron['creationDate']
    content_detail['lastUpdate'] = content_details_from_thron['lastUpdate']

    logger.debug((f'details: {content_detail}',content_detail))
    return content_detail



def write_to_ddb(itemsFromThron,contentIngestDate=date.today()):
    '''
    Creates the content cache mappings
    :param dict items: The list of video content from Thron
    :return:
    '''
    elementCount = len(itemsFromThron)
    logger.info(f"itemsFromThron :{elementCount} ")
    alreadyProcessedIds = []
    table = dynamodb.Table(content_table)
    with table.batch_writer() as writer:
    # Capture the Thron URLs for the different videos available and for each get details
        for thronItem in itemsFromThron:
            thronChannelTypeToMatch = contentTypeToThronChannelToSearch[thronItem["content"]['contentType'].lower()]
            id = thronItem["content"]["id"]
            for contents in thronItem["deliveryInfo"]:
                if contents["channelType"] == thronChannelTypeToMatch:
                    if id not in alreadyProcessedIds:
                        try:
                            tags = ''
                            if thronItem.get('itagDefinitions'):
                                for itagDefinition in thronItem["itagDefinitions"]:
                                    for tag in [x['label'] for x in itagDefinition["names"] if 'EN' in x['lang']]:
                                        tags=tags+"|"+tag
                                    if tags.startswith("|"):
                                        tags = tags.removeprefix("|")
                            contentDetails = get_content_details(id)
                            if contentDetails:
                                writer.put_item(Item={
                                    'contentId': id,
                                    'contentURL': contentDetails['contentUrl'],
                                    'contentType': contentDetails['contentType'],
                                    'contentIngestDate': contentIngestDate.strftime("%Y-%m-%d"),
                                    'contentMetadata' : {
                                        'name_title' : contentDetails['name_title'] ,
                                        'description' : contentDetails['description'],
                                        'thumb':  contentDetails['thumbUrl'] ,
                                        'durationMs' : contentDetails['durationMs'],
                                        'tags' : tags,
                                        'creationDate': contentDetails['creationDate'],
                                        'lastUpdate' : contentDetails['lastUpdate']
                                    }
                                })
                            alreadyProcessedIds.append(id)
                        except Exception as e :
                            logger.error((f'errors getting or writing detail for {id}'))
                            logger.exception(e)
