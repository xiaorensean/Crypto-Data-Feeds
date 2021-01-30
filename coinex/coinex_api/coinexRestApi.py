import requests
import os
import sys

ROOT_URL = "https://api.coinex.com/v1"

headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
    "Accept": "application/json",
    }


def get_tickers():
    url = ROOT_URL + "/market/list"
    response = requests.get(url,headers=headers)
    data = response.json()
    return data


def get_market_statistics(symbol=None):
    if symbol is not None:
        url = ROOT_URL + "/market/ticker?market={}".format(symbol)
    else:
        url = ROOT_URL + "/market/ticker/all"
    response = requests.get(url,headers=headers)
    data = response.json()
    return data


def get_orderbook(symbol):
    url = ROOT_URL + "/market/depth?market={}&limit=50&merge=0".format(symbol)
    response = requests.get(url,headers=headers)
    data = response.json()
    return data


def get_trades(symbol):
    url = ROOT_URL + "/market/deals?market={}".format(symbol)
    response = requests.get(url,headers=headers)
    data = response.json()
    return data


if __name__ == "__main__":
    market_info = get_tickers()['data']
    tickers = [i for i in market_info if "HNS" in i]
    market_statistic = get_market_statistics()
    orderbook = get_orderbook('hnsusdt')
    trades = get_trades('hnsusdt')

