import json
from botocore.vendored import requests
import urllib
from bs4 import BeautifulSoup
import traceback
import boto3
import asyncio

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


cfg = {
    's3': {
        'bucket': "emptortrial-docs",
    },
    'dyndb': {
        'table': 'emptortrial-titles'
    }
}


def title_process(bucket, table, key, url):
    '''
    process item from Table "table"; of ID "key"; retrieves html docuemnt from "url"
    stores it in s3 Bucket, and parses title, stores that into dynamodb "table" and updates item status to "PROCESSED"
    :param bucket: s3 bucket to store retrieved html document
    :param table: dynamodb table to store results
    :param key: item id in table
    :param url: html document url
    :return: item update result
    '''
    logger.info("BGN " + key)
    rsp = requests.get(url)
    soup = BeautifulSoup(rsp.content, 'html.parser')
    doctitle = soup.title.string
    # doctitle = str(hash(rsp.content))

    bucket.put_object(bucket.name, Key=key, Body=rsp.content)
    s3url = 'https://{bucket}.s3.amazonaws.com/{key}'.format(bucket=cfg['s3']['bucket'], key=key)

    return table.update_item(Key={'id': key}, AttributeUpdates={ 's3url': {'Value': str(s3url)}, 'title': {'Value': str(doctitle)},
                                                                'status': {'Value': 'PROCESSED'} } )


def title(event, context):
    '''
    function loops through records in "event", and process new entries which status is "PENDING"
    :param event: dynamodb stream event
    :param context:
    :return: None
    '''
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(cfg['dyndb']['table'])

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(cfg['s3']['bucket'])

    # TODO, async
    for record in event['Records']:
        try:
            if 'INSERT' != record['eventName'] or 'PENDING' != record['dynamodb']['NewImage']['status']['S']:
                logger.info("SKIPPED " + json.dumps(record))
                continue

            item = record['dynamodb']['NewImage']
            res = title_process(bucket, table, item['id']['S'], item['url']['S'])

            logger.info(res)

        except Exception as ex:
            logger.exception(ex)


def title_submit(event, context):
    '''
    :param event: api proxy event, contains query parameter 'url'
    :param context:
    :return: id for processing item in dynamodb titles table
    '''
    tb = ''
    try:

        url = event['queryStringParameters']['url']
        url = urllib.parse.unquote(url)
        key = str(hash(url))

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])

        table.put_item(Item={'id': key, 'url': url, 'status': 'PENDING'})

        return {
            'statusCode': 200,
            'body': json.dumps({"id": key}),
            'headers': {
                'Content-Type': 'application/json',
            }
        }


    #TODO; more info in error handling
    except Exception as ex:
        tb = traceback.format_exc()

    return {
        'statusCode': 200,
        'body': {"error": tb},
        'headers': {
            'Content-Type': 'application/json',
        }
    }


def title_get(event, context):
    '''
    :param event: api proxy event, contains query parameter 'id'
    :param context:
    :return: processing item in dynamodb titles table, of the specified id
    '''
    tb = ''
    try:

        key = event['queryStringParameters']['id']

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])
        res = table.get_item( Key={'id': key})
        return {
            'statusCode': 200,
            'body': json.dumps({"item": res['Item']}),
            'headers': {
                'Content-Type': 'application/json',
            }
        }

    #TODO; more info in error handling
    except Exception as ex:
        tb = traceback.format_exc()

    return {
        'statusCode': 200,
        'body': {"error": tb},
        'headers': {
            'Content-Type': 'application/json',
        }
    }
