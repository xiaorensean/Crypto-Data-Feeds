import requests
import os
import sys

BASE_URL = "https://api.wazirx.com"

def get_market_status():
    ping = BASE_URL + "/api/v2/market-status"
    response = requests.get(ping)
    data = response.json()
    return data


def get_market_tickers():
    ping = BASE_URL + "/api/v2/tickers"
    response = requests.get(ping)
    data = response.json()
    return data


def get_depth(symbol):
    ping = BASE_URL + "/api/v2/depth?symbol={}".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data


def get_trades(symbol):
    ping = BASE_URL + "/api/v2/trades?symbol={}".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data


if __name__ == "__main__":
    data = get_market_tickers()
    datas = []
    for d in data:
        datas.append(data[d])
    tickers = [i['name'] for i in datas]
    