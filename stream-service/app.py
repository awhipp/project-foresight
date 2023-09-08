'''Streaming service for getting FOREX data to a data store.'''
import json
import os

import requests
import psycopg2
import psycopg2.extras

import dotenv
dotenv.load_dotenv()

# Connection parameters
db_params = {
    'host': os.getenv('TIMESCALE_HOST'),  # Replace with your TimescaleDB host
    'port': os.getenv('TIMESCALE_PORT'),  # Replace with your TimescaleDB port
    'database': os.getenv('TIMESCALE_DB'),  # Replace with your database name
    'user': os.getenv('TIMESCALE_USER'),      # Replace with your database user
    'password': os.getenv('TIMESCALE_PASSWORD')  # Replace with your database password
}

# Establish a connection
# ! TODO - Move to a singleton instance
conn = None
try:
    conn = psycopg2.connect(**db_params, cursor_factory=psycopg2.extras.DictCursor)
    print("Connected to TimescaleDB")
except Exception as e:
    print(f"Error: {e}")

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

    cursor = conn.cursor()

    for line in requests.get(url, headers=head, stream=True, timeout=30).iter_lines():
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
                send_record(record, cursor)
    cursor.close()

def send_record(record: dict, cursor: psycopg2.extensions.cursor):
    '''Send a record to the data store.'''
    cursor.execute("""INSERT INTO forex_data (instrument, time, bid, ask)
        VALUES (%s, %s, %s, %s)""",
        (record['instrument'], record['time'], record['bid'], record['ask']))  
    conn.commit()
    print(record)

def create_table():
    '''Create a table in the data store.'''

    # Create a cursor
    cursor = conn.cursor()

    # Execute SQL queries here
    cursor.execute("""CREATE TABLE IF NOT EXISTS forex_data (
        instrument VARCHAR(10) NOT NULL,
        time TIMESTAMPTZ NOT NULL,
        bid FLOAT NOT NULL,
        ask FLOAT NOT NULL,
        PRIMARY KEY (instrument, time)                   
    )""")

    try:
        cursor.execute("""SELECT create_hypertable('forex_data', 'time')""")
    except psycopg2.DatabaseError:
        print("Already created the hyper table. Skipping.")

    conn.commit()

    cursor.execute("""DELETE FROM forex_data""")

    # Close the cursor and connection
    cursor.close()

if __name__ == '__main__':
    if conn is not None:
        create_table()
        open_stream()
        conn.close()