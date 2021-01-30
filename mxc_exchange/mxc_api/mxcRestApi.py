import time
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

ROOT_URL = "https://www.mxc.io"

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
    "Accept": "application/json",
    }



# get market 
def get_market():
    url = ROOT_URL + "/open/api/v1/data/markets" 
    response = requests.get(url,headers=headers)
    data = response.json()
    return data

# get market 
def get_market_info():
    url = ROOT_URL + "/open/api/v1/data/markets_info" 
    response = requests.get(url,headers=headers)
    data = response.json()
    return data

# api to get all trades
def get_trades(symbol):
    params = {'market': symbol}
    url = ROOT_URL + "/open/api/v1/data/history"
    response = requests.get(url,params=params,headers=headers)
    data = response.json()
    return data

# api to get orderbook
def get_orderbook(symbol):
    params = {'market': symbol}
    url = ROOT_URL + "/open/api/v1/data/depth"
    response = requests.get(url,params=params,headers=headers)
    data = response.json()
    return data

# api to get ticker
def get_ticker(symbol):
    params = {'market': symbol}
    url = ROOT_URL + "/open/api/v1/data/ticker"
    response = requests.get(url,params=params,headers=headers)
    data = response.json()
    return data

# api to get kline
def get_kline(symbol):
    params = {'market': symbol,
          'interval': '1m',
          'startTime': int(time.time() / 60) * 60 - 60 * 5,
          'limit': 5}
    url = ROOT_URL + '/open/api/v1/data/kline'
    response = requests.get( url, params=params, headers=headers)
    data = response.json()
    return data
    
if __name__ == "__main__":
    a = get_orderbook("HNS_USDT")
    
    
    
    