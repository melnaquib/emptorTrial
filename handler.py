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
        'bucket': "emptortrial-content",
    },
    'dyndb': {
        'table': 'emptorTrial_titles'
    }
}


def title_process(bucket, table, key, url):
    logger.info("BGN " + key)
    rsp = requests.get(url)
    soup = BeautifulSoup(rsp.content, 'html.parser')
    doctitle = soup.title.string
    # doctitle = str(hash(rsp.content))

    bucket.put_object(bucket.name, Key=key, Body=rsp.content)
    s3url = 'https://{bucket}.s3.amazonaws.com/{key}'.format(bucket=cfg['s3']['bucket'], key=key)

    return table.update_item(Key={'id': key}, AttributeUpdates={'s3url': {'Value': s3url}, 'title': {'Value': doctitle},
                                                                'status': {'Value': 'PROCESSED'}})


def title(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(cfg['dyndb']['table'])

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(cfg['s3']['bucket'])

    # TODO, async
    for record in event['Records']:
        try:
            if 'INSERT' != record['eventName'] or 'PENDING' != record['dynamodb']['NewImage']['status']:
                logger.info("SKIPPED " + json.dumps(record))
                continue

            item = record['dynamodb']['NewImage']
            res = title_process(bucket, table, list(item['Id'].values())[0], item['url'])
            logger.info(res)

        except Exception as ex:
            logger.exception(ex)


def title_submit(event, context):
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
