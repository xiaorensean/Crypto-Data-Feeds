import time
import websocket
import json
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Restful API 
url_base_rest = "https://www.namebase.io/api/v0/"

# websocket api
url_base_ws = "wss://app.namebase.io/ws/v0/"


def get_trade(symbol,ts):
    trade_url = url_base_rest +  "trade?symbol={}&timestamp={}&receiveWindow=1000&limit=1000".format(symbol,ts)
    response = requests.get(trade_url)
    resp = response.json()
    return resp

def get_ticker(symbol):
    trade_url = url_base_rest +  "ticker/day?symbol={}".format(symbol)
    response = requests.get(trade_url)
    resp = response.json()
    return resp    

def get_depth(symbol):
    depth_url = url_base_rest + "depth?symbol={}".format(symbol)
    response = requests.get(depth_url)
    resp = response.json()
    return resp    


def depth_ws():
    depth_ws_url = url_base_ws + "ticker/depth"
    while True:
        ws = websocket.create_connection(depth_ws_url)
        recv = ws.recv()
        data = json.loads(recv)
        print(data)

def trade_ws():
    trade_ws_url = url_base_ws + "stream/trades"
    while True:
        ws = websocket.create_connection(trade_ws_url)
        recv = ws.recv()
        data = json.loads(recv)
        print(data)

def kline_ws():
    kline_ws_url = url_base_ws + "ticker/kline_5m"
    while True:
        ws = websocket.create_connection(kline_ws_url)
        recv = ws.recv()
        data = json.loads(recv)
        print(data)

        
if __name__ == "__main__":
    pass
#    while True:
#        try:
#            trade_ws()
#        except Exception as err:
#            print(str(err))
#            time.sleep(10)
    
#symbol = "HNSBTC"
#depth_data = get_depth(symbol)
#current_time = str(int(time.time()*1000))
#ts = 1581012838293
#a = get_trade(symbol,current_time)
#a1 = get_depth(symbol)
#a2 = get_ticker(symbol)
