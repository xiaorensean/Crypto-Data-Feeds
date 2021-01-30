import numpy as np
import datetime
import random
import requests
import time
import traceback

# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from okex_api.futures_api import FutureAPI
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()

okex = FutureAPI('', '', '')

measurement = "okex_orderbook"

def get_tickers(data):
    symbol_data = []
    for d in data:
        symbol_data += d['contracts']
    symbol = [i['name'] for i in symbol_data]  
    return symbol

# function to automatical fetch all tickers
def get_future_tickers():
    base = "https://www.okex.com/v3/"
    future_tickers_endpoint = "futures/pc/market/futuresCoin?currencyCode=0"
    future_tickers = base + future_tickers_endpoint
    response = requests.get(future_tickers)
    resp = response.json()
    data = resp['data']
    #symbol_usdt = get_tickers(data['usdt'])
    symbol_raw = get_tickers(data['usdt'])
    symbol_usd = [sr.split("-")[0] + "-" + sr.split("-")[1].split("T")[0] + "-" + str(datetime.datetime.now())[:2] + sr.split("-")[1].split("T")[1] for sr in symbol_raw]
    symbol_usdt = [sr.split("-")[0] + "-" + sr.split("-")[1].split("T")[0] + "T-" + str(datetime.datetime.now())[:2] + sr.split("-")[1].split("T")[1] for sr in symbol_raw]
    return symbol_usd, symbol_usdt


def write_to_influxdb(order_book_type,measurement,data,symbol):
    fields = {}
    for entry in data[order_book_type]:
        dbtime = False
        tags = {}
        tags.update({'symbol':symbol})
        fields['price_depth'] = float(entry[0])
        fields['price_amount']  = float(entry[1])
        fields['orders_depth_liquidated'] = float(entry[2])
        fields['orders_depth'] = float(entry[3])
        fields['type'] = order_book_type
        try:
            ts = " ".join(data['timestamp'].split("Z")[0].split("T"))
        except:
            ts = " ".join(data['time'].split("Z")[0].split("T"))
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        uts = int(time.mktime(dt_temp.timetuple()) * 1000 + random.random())
        fields['ref_ts'] = uts
        fields.update({"is_api_return_timestamp": True})
        influxdb.write_points_to_measurement(measurement, dbtime, tags, fields)

def subscribe_orderbook_future_usdt_cluster_1():
    fut_tickers = sorted(get_future_tickers()[1])
    leng = len(fut_tickers)/2
    idx = int(np.ceil(leng))
    fut_tickers_cluster_1 = fut_tickers[:idx]
    fut_tickers_cluster_2 = fut_tickers[idx:]

    for symb in fut_tickers_cluster_1:
        try:
            data = okex.get_depth(symb, 200)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,symb)
            return 
        write_to_influxdb('bids', measurement, data, symb)
        write_to_influxdb('asks', measurement, data, symb)

          
if __name__ == '__main__':
    subscribe_orderbook_future_usdt_cluster_1()
    while True:
        time.sleep(35)
        subscribe_orderbook_future_usdt_cluster_1()


