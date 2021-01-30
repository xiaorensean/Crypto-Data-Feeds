import traceback
import os
import sys
import datetime
import time
import requests
import websocket
import json
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(current_dir))

from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

# websocket
def connection(message):
    msg_init = {"payload": {},"type": "connection_init"}
    msg_stop = {"payload": {},"type":"stop"}
    ws_url = "wss://api.thegraph.com/subgraphs/name/balancer-labs/balancer"
    ws = websocket.create_connection(ws_url)
    ws.send(json.dumps(msg_init))
    ws.send(json.dumps(message))
    response = ws.recv()
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    return data



url = "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer"

#param = {"query":"{pools (first: 100,  skip: 0,  where: {  finalized: false,    tokensList_not: []          },            orderBy: liquidity,          orderDirection: desc,             ) {   id        publicSwap      finalized        swapFee        totalWeight        totalShares        totalSwapVolume        tokensList        tokens {           id            address            balance            decimals           symbol         denormWeight     }        swaps (           first: 1,            orderBy: timestamp,            orderDirection: desc,            where: {               timestamp_lt: 1596500000      }    ) {            tokenIn           tokenInSym            tokenAmountIn            tokenOut            tokenOutSym            tokenAmountOut      poolTotalSwapVolume        }    }}"}
param = {"query":"{ pools(first: 1000, where: {id: '0x0e511aa1a137aad267dfe3a6bfca0b856c1a3682'}) {    id    finalized    publicSwap    swapFee   totalWeight    tokensList    tokens {      id      address      balance      decimals      symbol\n      denormWeight    }  }}","variables":{}}
response = requests.post(url,data=param)
#response = requests.get(url)
data = response.content
print(data)

data = connection(param)
print(data)

