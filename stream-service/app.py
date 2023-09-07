'''Streaming service for getting FOREX data to a data store.'''
import json
import os

import requests

import dotenv
dotenv.load_dotenv()

def open_stream():
    '''Open a stream to the OANDA API and send the data to the data store.
    '''
    account_id = os.getenv('OANDA_API_ACCOUNT_ID')
    api_token = os.getenv('OANDA_API_TOKEN')

    url = f'https://stream-fxtrade.oanda.com/v3/accounts/{account_id}/pricing/stream?instruments=EUR_USD'
    head = {
        'Content-type':"application/json",
        'Accept-Datetime-Format':"RFC3339",
        'Authorization':f"Bearer {api_token}"
    }

    r = requests.get(url, headers=head, stream=True, timeout=30)

    for line in r.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            obj = json.loads(decoded_line)
            if obj['type'] == 'PRICE' and obj['tradeable']:
                record = {
                    'instrument': obj['instrument'],
                    'time': obj['time'],
                    'bid': float(obj['bids'][0]['price']),
                    'ask': float(obj['asks'][0]['price'])
                }
                send_record(record)

def send_record(record: dict):
    '''Send a record to the data store.'''
    print(record)


if __name__ == '__main__':
    open_stream()
