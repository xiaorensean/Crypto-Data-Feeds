#!/usr/bin/env python

import asyncio
import websockets
import websocket
import json
import requests
import dateutil.parser as dp
import hmac
import base64
import zlib
import re
import copy
import datetime as dt
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BMonthEnd, Week, Day, BQuarterEnd

from termcolor import colored
from time import sleep

import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) )))
from influxdb_client.okex_influxdb_client import OkexInfluxClient


channels = {}

suffixes = {}

endpoints = [
    "futures/ticker:",
    "futures/price_range:",
    "futures/mark_price:",
    "futures/trade:",
]

pairs = [
    "BTC-USD-",
    "ETH-USD-",
    "LTC-USD-",
    "BTC-USDT-",
    "ETH-USDT-",
    "LTC-USDT-"
]

past_5_trades = {
    "btc_weekly": [],
    "btc_biweekly": [],
    "btc_quarterly": [],

    "eth_weekly": [],
    "eth_biweekly": [],
    "eth_quarterly": [],

    "ltc_weekly": [],
    "ltc_biweekly": [],
    "ltc_quarterly": [],
}

next_reset_time = None

'''
OKex functions
'''

def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""

def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp


def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

'''
Futures-specific functions
'''

#gets next expiry date of futures
def get_expiry_okex(current_date):
    
    if current_date.hour < 8 and (0 < current_date.hour or 0 < current_date.minute): # 00:01 TO 07:59
        rounded_date = current_date.replace(microsecond=0,second=0,minute=0,hour=0)
    else:
        rounded_date = (current_date + pd.Timedelta("1 day")).replace(microsecond=0,second=0,minute=0,hour=0)

    times = []
    suffixes = []
    
    times.append(Week(weekday=4).rollforward(rounded_date) + pd.Timedelta("8 hr"))
    times.append(Week(weekday=4).rollforward(rounded_date + pd.Timedelta('7 days')) + pd.Timedelta('8 hr'))
    next_quarter = pd.to_datetime(BQuarterEnd().rollforward(rounded_date + pd.Timedelta("1 day"))) + pd.Timedelta("8 hr")
    second_quarter = pd.to_datetime(BQuarterEnd().rollforward(next_quarter + pd.Timedelta("1 day"))) + pd.Timedelta("8 hr")
    #round second quarter to the past Friday
    dow = second_quarter.date().weekday()
    gap = (dow - 4)%7
    second_quarter = second_quarter - dt.timedelta(days = gap)
    times.append(second_quarter)
    # round last quarter to the past Friday
    third_quarter = pd.to_datetime(BQuarterEnd().rollforward(second_quarter + pd.Timedelta("1 day"))) + pd.Timedelta("8 hr")
    last_quarter = pd.to_datetime(BQuarterEnd().rollforward(third_quarter + pd.Timedelta("1 day"))) + pd.Timedelta(
        "8 hr")
    dow = last_quarter.date().weekday()
    gap = (dow - 4) % 7
    last_quarter = last_quarter - dt.timedelta(days=gap)
    times.append(last_quarter)

    for time in times:
        word = str(time)
        suffix = word[0:4] + word[5:7] + word[8:10]
        suffixes.append(suffix)

    return suffixes

#checks if time is Friday 8AM, because then we need to update the futures tickers
def is_reset_time(current_date):
    return current_date > next_reset_time

'''
Main functions
'''

def initialise_channels():
    for pair in pairs:
        for key in suffixes.keys():
            types = []
            for endpoint in endpoints:
                types.append(endpoint + pair + suffixes[key])
            channels.update({pair + key : types})
    pass

def initialise():
    current_time = dp.parse(get_server_time())
    times = get_expiry_okex(current_time)
    suffixes.update({"weekly": times[0]})
    suffixes.update({"biweekly": times[1]})
    suffixes.update({"quarterly": times[2]})
    initialise_channels()
    pass

def subscribe_without_login(url, client):
    ws = websocket.create_connection(url)
    for key in channels.keys():
        for symbol in channels[key]:
            sub_param = {"op": "subscribe", "args": symbol}
            sub_str = json.dumps(sub_param)
            ws.send(sub_str)
            print(f"send: {sub_str}")

    while (True):
        try:
            res = ws.recv()
            res = inflate(res)
            response = json.loads(res)
            try:
                response_type = response["table"]
            except:
                #Initial response to confirm subscription
                try:
                    response_type = response["event"]
                    continue
                #Some kind of error
                except:
                    print(colored("Invalid response from server", "red"))
                    ws.close()
                    reconnect(url, channels)

            symbol = response["data"][0]["instrument_id"]
            endpoint = response_type.split("/")[1]
            date = symbol.split("-")[2]
            expiration = None
            for key in suffixes.keys():
                if suffixes[key] == date:
                    expiration = key
            data = response["data"]

            #Write to db, check for errors
            if not client.write(endpoint, symbol, data, expiration):
                print(colored("Database error!", "yellow"))
                client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "okex/subscribe_futures_data.py"))
                client = OkexInfluxClient(channels)
                reconnect(url, client)

        except:
            ws.close()
            client.send_email("Subject: {}\n\n{}".format("ERROR with Script", "okex/subscribe_futures_data.py"))
            reconnect(url, client)

def reconnect(url, client):
    print (colored("Reconnecting","red"))
    sleep(1)

    subscribe_without_login(url, client)

api_key = ''
seceret_key = ''
passphrase = ''
url = 'wss://real.okex.com:8443/ws/v3'

if __name__ == "__main__":
    initialise()
    client = OkexInfluxClient(channels)
    subscribe_without_login(url, client)
