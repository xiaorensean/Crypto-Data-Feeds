import websocket
import json
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata


import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) )))
from influxdb_client.deribit_influxdb_client import DeribitInfluxClient

tickers = [
  "BTC", 
  "ETH"
]
endpoints = [
  "ticker",
]

swap_ep = metadata.get_swap_endpoints()
future_ep = metadata.get_futures_endpoints(tickers, endpoints)
option_ep = metadata.get_options_endpoints(tickers, endpoints)
endpoint = option_ep[0]


def connection_test(endpoint):
    msg = \
    {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": [endpoint]
      }
    }
    ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
    ws.send(json.dumps(msg))


ticker_ava = []
ticker_unava = []
for oep in option_ep[30:60]:
    try:
        connection_test(oep)
        ticker_ava.append(oep)
    except:
        ticker_unava.append(oep)
        pass