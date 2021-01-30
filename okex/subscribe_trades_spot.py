import time
import traceback
import datetime
import random

# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from okex_api.spot_api import SpotAPI
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()

okex_spot = SpotAPI('', '', '')



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


def subscribe_trades_spot():
    measurement = "okex_trades"
    symbols = ["HBAR-USDT","HBAR-BTC","HBAR-USDK","ALGO-USDT","ALGO-USDK",\
           "ALGO-BTC","ALGO-ETH","BTC-USDT","ETH-USDT"]
    for symb in symbols:
        try:
            data = okex_spot.get_trades(symb)
        except:
            error = traceback.format_exc()
            #logger(measurement,error,symb)
            data = []
        write_trade_data(measurement, data, symb)


if __name__ == "__main__":
    subscribe_trades_spot()
    while True:
        time.sleep(30)
        subscribe_trades_spot()

