'''
Subscribe funding rates
'''

import time 
import websocket
import json
#import nest_asyncio
import sys
from os import path
sys.path.append(path.dirname(path.abspath(__file__)))
from deribit_api.deribitRestApi import RestClient

sys.path.append(path.dirname( path.dirname( path.abspath(__file__) )))
from influxdb_client.influxdb_client_v1 import InfluxClient

db = InfluxClient()
deribit = RestClient()

#symbols = ["BTC-PERPETUAL", "ETH-PERPETUAL"]
#data = deribit.getsummary(symbols[1])

ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
msg = {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": ["perpetual.BTC-PERPETUAL.raw"]}
    }
ws.send(json.dumps(msg))
response = json.loads(ws.recv())
data =  json.loads(ws.recv())
ws.close