import os
import sys
import requests

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


base_url = "https://data.gateio.life/api2/1/"


def get_marketinfo():
    orderbook_endpoint = base_url + "marketinfo"
    response = requests.get(orderbook_endpoint)
    data = response.json()
    return data


def get_orderbook(symbol):
    orderbook_endpoint = base_url + "orderBook/" + symbol
    response = requests.get(orderbook_endpoint)
    data = response.json()
    return data


def get_trades(symbol,tid=None):
    if tid is None:
        orderbook_endpoint = base_url + "tradeHistory/" + symbol
    else:
        orderbook_endpoint = base_url + "tradeHistory/" + symbol + "/" + str(tid)
    response = requests.get(orderbook_endpoint)
    data = response.json()
    return data



if __name__ == "__main__":
    #order_book = get_orderbook("hns_usdt")
    #trade = get_trades("hns_usdt")
    trade_id = get_trades("hns_usdt")