import time
import datetime
import random
import traceback

# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from okex_api.swap_api import SwapAPI
from subscribe_oi_swaps import get_swap_tickers
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()

okex_swap = SwapAPI('', '', '')

def write_trade_data(measurement,data,symbol):
    for d in data:
        fields = {}
        fields.update({"trade_id":int(d["trade_id"])})
        fields.update({"side":d["side"]})
        fields.update({"price":float(d['price'])})
        fields.update({"qty":float(d["size"])})
        fields.update({"is_api_return_timestamp": True})
        ts = " ".join(d['timestamp'].split("Z")[0].split("T"))
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        uts = int(time.mktime(dt_temp.timetuple()) * 1000 + random.random())
        dbtime = datetime.datetime.utcfromtimestamp(uts/1000)
        tags = {}
        tags.update({"symbol":symbol})
        influxdb.write_points_to_measurement(measurement, dbtime, tags, fields)



def subscribe_trades_swap_usd():
    measurement = "okex_trades"
    swap_tickers_usd = [i['contract'] for i in get_swap_tickers("USD")]
    for symb in swap_tickers_usd:
        try:
            data = okex_swap.get_trades(symb)   
        except:
            error = traceback.format_exc()
            #logger(measurement,error,symb)
            data = []
        write_trade_data(measurement, data, symb)


if __name__ == "__main__":
    subscribe_trades_swap_usd()
    while True:
        time.sleep(30)
        subscribe_trades_swap_usd()


