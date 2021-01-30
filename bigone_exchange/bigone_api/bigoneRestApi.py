import requests
import os
import sys

BASE_URL = "https://big.one/api/v3"


def get_server_timestamp():
    ping = BASE_URL + "/ping"
    response = requests.get(ping)
    data = response.json()
    return data


def get_assetpair():
    ping = BASE_URL + "/asset_pairs"
    response = requests.get(ping)
    data = response.json()
    return data


def get_ticker(symbol):
    ping = BASE_URL + "/asset_pairs/{}/ticker".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data


def get_tickers():
    ping = BASE_URL + "/asset_pairs/tickers"
    response = requests.get(ping)
    data = response.json()
    return data


def get_orderbook(symbol):
    ping = BASE_URL + "/asset_pairs/{}/depth?limit=200".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data


def get_trades(symbol):
    ping = BASE_URL + "/asset_pairs/{}/trades".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data


def get_candles(symbol):
    ping = BASE_URL + "/asset_pairs/{}/candles".format(symbol)
    response = requests.get(ping)
    data = response.json()
    return data



if __name__ == "__main__":
    ts = get_server_timestamp()
    ticker = get_ticker("HNS-BTC")
    tickers = get_tickers()
    orderbook = get_orderbook("HNS-BTC")
    trades = get_trades("HNS-BTC")