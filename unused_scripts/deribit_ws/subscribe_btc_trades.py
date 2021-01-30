'''
Subscribe to trades for all BTC-based swaps, futures, and options
'''

import asyncio
import websockets
import json
#import nest_asyncio

import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) )))
from influxdb_client.deribit_influxdb_client import DeribitInfluxClient

TRADE_HISTORY = 500
past_orders = []

msg = \
{
  "jsonrpc" : "2.0",
  "id" : 9290,
  "method" : "public/get_last_trades_by_currency",
  "params" : {
    "currency" : "BTC",
    "count" : TRADE_HISTORY
  }
}

async def call_api(msg, client):
   async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg)
       response = await websocket.recv()
       # do something with the response...
       orders = json.loads(response)['result']['trades']
       new_orders = []
       for order in orders:
        if (order in past_orders):
          continue
        else:
          new_orders.append(order)
          past_orders.append(order)
          if (len(past_orders) > TRADE_HISTORY):
              past_orders.pop(0)
       past_orders.sort(key = lambda x:x["timestamp"])
       new_orders.sort(key = lambda x:x["timestamp"])

       client.write("trades", None, new_orders)
       

       
client = DeribitInfluxClient()
#nest_asyncio.apply()
while (True):
  asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg), client))