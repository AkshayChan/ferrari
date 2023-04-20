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
import http.client
import json
import logging
import os
from urllib.parse import urlparse
from datetime import datetime, timedelta

import common_http_client_util as httpclientUtil
import boto3

"""Initialise variables"""
logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')
content_table = os.environ['CONTENT_TABLE']
cms_apy_key =  os.environ['CMS_API_KEY']
cms_host = urlparse(os.environ['CMS_ENDPOINT']).hostname
cdn_host = os.environ['CDN_HOST']
cms_fan_app_basepath = os.environ['CMS_BASE_PATH']
cms_fan_app_news_path = cms_fan_app_basepath + "/fan-app-news/published"
CMS_FAN_APP_NEWS_QUERY_STRING_FORMAT = (
    "locale=en" +
    "&skip={skip}" +
    "&limit={limit}" +
    "&scuderia=true" +
    "&categories=scuderia")
"""&sinceDate=2022-06-28T10:29:30"""

CMS_FAN_APP_NEWS_ITEMS_PER_PAGE = 500


def handler(event, context):
    '''
    Lambda handler function
    :return:
    '''
    # logger.info(f'Processing {str(len(event["Records"]))} records in this invocation')

    maxItems = event.get('maxItems',0)
    daysAgo = int(os.environ.get('DAYS_AGO',event.get('daysAgo',0))) #env variable has priority
    if (daysAgo == 0):
        logger.info(f'Extracting CMS data for initial update')
    else:
        logger.info(f'Extracting CMS data for partial update (daysAgo %d)',daysAgo)
    if (maxItems > 0):
        logger.info(f'results will be limited to %d as required via event parameter',maxItems)
    pageSize =  min(maxItems,CMS_FAN_APP_NEWS_ITEMS_PER_PAGE) if (maxItems>0) else CMS_FAN_APP_NEWS_ITEMS_PER_PAGE
    skip = 0
    readPage =  readPagedCMSNews(skip,pageSize,daysAgo)
    totalItems = readPage.get('totalItems',0)
    totalItems = min(totalItems,maxItems) if (maxItems>0) else totalItems
    if totalItems > 0 :
        items = readPage["items"]
        items = (items[:(maxItems)]) if ( maxItems > 0 and len(items) > maxItems) else items
        contentIdsWrittenSoFar = []
        logger.info('Start writing (%s)/(%s) items ',len(items), totalItems)
        write_to_ddb(items,contentIdsWrittenSoFar)
        logger.info('Written (%s)/(%s) so far ',skip+len(items), totalItems)
        skip += pageSize
        while skip < totalItems:
            readPage =  readPagedCMSNews(skip,pageSize,daysAgo)
            items = readPage["items"]
            items = (items[:maxItems-(skip+len(items))]) if ( maxItems > 0 and skip+len(items) > maxItems) else items
            logger.info('Continue writing (%s) items',len(items))
            write_to_ddb(items,contentIdsWrittenSoFar)
            logger.info('Written (%s)/(%s) so far ',skip+len(items), totalItems)
            skip += pageSize
        logger.info('End writing all (%s) items ', totalItems)
        if (totalItems>len(contentIdsWrittenSoFar)):
            logger.warning("Got %s elements skipped due to duplicated in CMS call!", (totalItems-len(contentIdsWrittenSoFar)))
    else:
      logger.warning('no items found in CMS!')
    

def readPagedCMSNews(skip = 0, itemsPerPage = 5, daysAgo = 0):
    '''
    Extracts the initial upload
    :return: dict items: The list of cms content from CMS
    '''
    #httpclientUtil.initHttpClientDebugLevel(1)
    readPage = { "totalItems": 0, "items" : [] }
    conn = http.client.HTTPSConnection(cms_host)
    payload = ''
    headers = {
        'x-api-key': cms_apy_key
    }
    queryString = CMS_FAN_APP_NEWS_QUERY_STRING_FORMAT.format(skip = skip, limit = itemsPerPage)
    if (daysAgo > 0):
        today = datetime.utcnow()
        delta = timedelta(days = daysAgo)
        sinceDate = today - delta
        logger.info('End writing daysAgo (%s), delta(%s), sinceDate (%s)', daysAgo,delta,sinceDate)
        queryString += "&sinceDate="+sinceDate.replace(microsecond=0).isoformat()
       
    logger.debug(f'asking CMS with query string %s',queryString)
    conn.request("GET", cms_fan_app_news_path+"?"+queryString, '', headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data.decode("utf-8"))
    # logger.debug(data)
    readPage["totalItems"] = data["total"]
    readPage["items"] = data["items"]
    return readPage

def write_to_ddb(items, contentIdsWrittenSoFar = [], contentIngestDate=datetime.utcnow()):
    '''
    Creates the content cache mappings
    :param dict items: The list of news content from CMS
    :return:
    '''

#
#    * *thumb*:  ${readVariableEnv('CDN_HOST')}/items[i].content.thumb.landscape.id
#* *thumbDesc*: items[i].content.thumb.landscape.alt ← DON’T KNOW IF APP REQUIRES IT
#* *time*: item.publishedAt ← SHOULD BE SIMILAR TO creationDate or lastUpdate (see Thron)
#* *channel*: items[i].content.channel
#* *tags*:  items[i].tags[].slug
#* *place*: items[i].content.place

    # Write the contentId and all other attributes to DynamoDB
    table = dynamodb.Table(content_table)
    with table.batch_writer() as writer:
        for item in items:
            tags = ''
            for tag in item['tags']:
                if 'slug' in tag:
                    tags=tags+"|"+tag['slug']
                    if tags.startswith("|"):
                        tags = tags.removeprefix("|")
            if 'slug' in item:
                channel =  item.get('content',{}).get('channel','')
                slug = item.get('slug',{})
                contentId = ( 'fan-app-news' if (channel == 'fan-app-news') else 'news') + '/published/' + slug
                if contentId in contentIdsWrittenSoFar:
                    logger.warning(f"GOT %s already present in this call! Skpping writing to Ddb",contentId)
                    continue
                writer.put_item(Item={
                    'contentId': contentId,
                    'contentURL': slug, #same as slug?
                    'contentType' : 'news',
                    'contentIngestDate': contentIngestDate.strftime("%Y-%m-%d"),
                    'contentMetadata' :  { 
                        'name_title' : item.get('title'),
                        'publishedAt' :  item.get('publishedAt'),
                        'subtitle' : item.get('content',{}).get('internalTitle',''),
                        'thumb':  cdn_host + "/"+ item.get('content',{}).get('thumb',{}).get('landscape',{}).get('id',''),
                        'thumbDesc':  item.get('content',{}).get('thumb',{}).get('landscape',{}).get('alt',''),
                        'channel':  item.get('content',{}).get('channel',''),
                        'tags' :  tags ,
                        'place' :  item.get('content',{}).get('place','')
                    }
                    }
                )
                contentIdsWrittenSoFar.append(contentId)
