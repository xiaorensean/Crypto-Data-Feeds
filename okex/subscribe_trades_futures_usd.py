import traceback
import time
import datetime
import random
# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from okex_api.futures_api import FutureAPI
from subscribe_orderbook_futures_usd1 import get_future_tickers
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()

okex_futures = FutureAPI('', '', '')

measurement = "okex_trades"


def write_trade_data(measurement,data,symbol):
    for d in data[0]:
        fields = {}
        fields.update({"trade_id":int(d["trade_id"])})
        fields.update({"side":d["side"]})
        fields.update({"price":float(d['price'])})
        fields.update({"qty":float(d["qty"])})
        fields.update({"is_api_return_timestamp": True})
        ts = " ".join(d['timestamp'].split("Z")[0].split("T"))
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        uts = int(time.mktime(dt_temp.timetuple()) * 1000 + random.random())
        dbtime = datetime.datetime.utcfromtimestamp(uts/1000)
        tags = {}
        tags.update({"symbol":symbol})
        influxdb.write_points_to_measurement(measurement, dbtime, tags, fields)



def subscribe_trades_futures_usd():
    tickers = get_future_tickers()
    tickers_usd = tickers[0]
    for t in tickers_usd:
        try:
            data = okex_futures.get_trades(t)
            print(data)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,t)
            data = []
        write_trade_data(measurement,data,t)

    

if __name__ == '__main__':
    subscribe_trades_futures_usd()
    while True:
        time.sleep(30)
        subscribe_trades_futures_usd()



