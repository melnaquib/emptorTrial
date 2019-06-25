import json
from botocore.vendored import requests
import urllib
from bs4 import BeautifulSoup
import traceback

import boto3


cfg = {
    's3': {
        'bucket': "emptortrial-content",
    },
    'dyndb': {
        'table': 'emptorTrial_titles'
    }
}


def title(event, context):
    tb = ''
    try:

        url = event['queryStringParameters']['url']
        url = urllib.parse.unquote(url)
        key = str(hash(url))

        rsp = requests.get(url)
        soup = BeautifulSoup(rsp.content, 'html.parser')
        doctitle = soup.title.string

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cfg['s3']['bucket'])

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(cfg['dyndb']['table'])

        table.put_item(Item={'id': key, 'title': doctitle})
        bucket.put_object(cfg['s3']['bucket'], Key=key, Body=rsp.content)

        return {
            'statusCode': 200,
            'body': json.dumps({"title": doctitle}),
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
