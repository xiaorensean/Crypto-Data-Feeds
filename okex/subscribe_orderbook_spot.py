import time
import random
import datetime
import traceback

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

measurement = "okex_orderbook"

def write_to_influxdb(order_book_type,measurement,data,symbol):
    fields = {}
    for entry in data[order_book_type]:
        dbtime = False
        tags = {}
        tags.update({'symbol':symbol})
        fields['price_depth'] = float(entry[0])
        fields['price_amount'] = float(entry[1])
        fields['orders_depth'] = float(entry[2])
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


def subscribe_orderbook_spot():
    symbols_spot = ["HBAR-USDT","HBAR-BTC","HBAR-USDK","ALGO-USDT","ALGO-USDK",\
           "ALGO-BTC","ALGO-ETH","BTC-USDT","ETH-USDT"]
    for symb in symbols_spot:
        try:
            data = okex_spot.get_depth(symb, 200)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,symb)
            return 
        write_to_influxdb('bids', measurement, data, symb)
        write_to_influxdb('asks', measurement, data, symb)

          
if __name__ == '__main__':
    subscribe_orderbook_spot()
    while True:
        time.sleep(27)
        subscribe_orderbook_spot()


