import json
from botocore.vendored import requests
import urllib
from bs4 import BeautifulSoup
import traceback

import boto3
import asyncio

cfg = {
    's3': {
        'bucket': "emptortrial-content",
    },
    'dyndb': {
        'table': 'emptorTrial_titles'
    }
}


async def title(key):
    tb = ''
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])
        res = table.get_item( Key={'id': key})
        item = res['Item']
        url = item['url']

        rsp = requests.get(url)
        soup = BeautifulSoup(rsp.content, 'html.parser')
        doctitle = soup.title.string

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cfg['s3']['bucket'])
        bucket.put_object(cfg['s3']['bucket'], Key=key, Body=rsp.content)

        s3url = 'https://{bucket}.s3.amazonaws.com/{key}'.format(bucket=cfg['s3']['bucket'], key=key)

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])
        table.update_item(Key={'id': key}, AttributeUpdates={'s3url': s3url, 'title': doctitle, 'status': 'PROCESSED'})


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


def title_submit(event, context):
    tb = ''
    try:

        url = event['queryStringParameters']['url']
        url = urllib.parse.unquote(url)
        key = str(hash(url))

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])

        table.put_item(Item={'id': key, 'url': url, 'status': 'PENDING'})

        asyncio.run(title(key))

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
