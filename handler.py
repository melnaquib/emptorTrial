import json
from botocore.vendored import requests
import urllib
from bs4 import BeautifulSoup
import traceback


def title(event, context):
    tb = ''
    try:

        url = event['queryStringParameters']['url']
        url = urllib.parse.unquote(url)

        rsp = requests.get(url)
        soup = BeautifulSoup(rsp.content, 'html.parser')
        title = soup.title.string

        return {
            'statusCode': 200,
            'body': json.dumps({"title": title}),
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
