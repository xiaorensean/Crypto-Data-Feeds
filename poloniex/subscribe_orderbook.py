import traceback
import multiprocessing as mp
import time
import os
import datetime
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from poloninex_api.poloniexRestApi import returnOrderBook
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient


influxdb = InfluxClient()

measurement = 'poloniex_orderbook'


def write_data(itype,symbol,orderbook):
    try:
        data = orderbook[itype]
    except:
        time.sleep(1)
        try:
            data = orderbook[itype]
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement, error_message,symbol)
            return    
    timestamp = time.time() * 1000
    if len(data) != 0:
        for ob in data:
            fields = {}
            fields.update({"price":float(ob[0])})
            fields.update({"amount":float(ob[1])})
            fields.update({"type":itype})
            fields.update({"ref_ts":int(timestamp)})
            fields.update({"is_api_return_timestamp": False})
            times = False
            tags = {}
            tags.update({"symbol":symbol})
            influxdb.write_points_to_measurement(measurement,times,tags,fields)
    else:
        pass


def asks_demands(symbol):
    orderbook = returnOrderBook(symbol)
    itype = "asks" 
    write_data(itype,symbol,orderbook)
    itype = "bids"
    write_data(itype,symbol,orderbook)


def subscribe_orderbook(symbol):
    asks_demands(symbol)
    while True:
        time.sleep(20)
        asks_demands(symbol)


symbols = ["USDT_SNX","TRX_SNX","BTC_SNX"]

for symb in symbols:
    collecting_symb = mp.Process(target=subscribe_orderbook, args=(symb,))
    collecting_symb.start()


