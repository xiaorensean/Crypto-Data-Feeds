import requests
import time
import json
import os
import datetime
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(current_dir)



url = 'https://poloniex.com/public'

def api_wraper(endpoint):
    response = requests.get(endpoint)
    response.status_code
    message = response.content
    data = json.loads(message)
    return data

def returnTicker():
    endpoint = url + '?command=returnTicker'
    data = api_wraper(endpoint)
    return data

def return24hVolume():
    endpoint = url + '?command=return24hVolume'
    data = api_wraper(endpoint)
    return data

def returnOrderBook(symbol):
    endpoint = url + '?command=returnOrderBook&currencyPair={}&depth=100'.format(symbol)
    data = api_wraper(endpoint)
    return data

def returnTradeHistory(symbol,start_ts=None,end_ts=None):
    if start_ts is not None and end_ts is not None:
        endpoint = url + '?command=returnTradeHistory&currencyPair={}&start={}&end={}'.format(symbol,start_ts,end_ts)
        data = api_wraper(endpoint)
    else:
        endpoint = url + '?command=returnTradeHistory&currencyPair={}'.format(symbol)
        data = api_wraper(endpoint)
    return data

def returnCurrencies():
    endpoint = url + '?command=returnCurrencies'
    data = api_wraper(endpoint)
    return data

def returnLoanOrders(symbol):
    funding_orderbooks_endpoint = url+'?command=returnLoanOrders&currency={}'.format(symbol)
    data = api_wraper(funding_orderbooks_endpoint)
    return data



if __name__ == "__main__":
    data = returnTradeHistory('TRX_SNX')