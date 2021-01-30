import time
import datetime
import random
import traceback
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from mxc_api.mxcRestApi import get_trades 
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "mxc_trades"

symbol = "HNS_USDT"


def trade_data(symbol,measurement):
    trades_data = get_trades(symbol)
    try:
        data = trades_data['data']
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,symbol)
    fields = {}
    for d in data:
        fields.update({"price":float(d["tradePrice"])})
        fields.update({"amount":float(d["tradeQuantity"])})
        fields.update({"is_api_return_timestamp": True})
        if d['tradeType'] == "1":
            fields.update({"side":"buy"})
        elif d['tradeType'] == "2":
            fields.update({"side":"sell"})
        dt_temp = datetime.datetime.strptime(d['tradeTime'], "%Y-%m-%d %H:%M:%S.%f")
        dt_temp = dt_temp - datetime.timedelta(1)
        uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
        dt = datetime.datetime.utcfromtimestamp(uts / 1000)
        dbtime = dt
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


if __name__ == "__main__":
    trade_data(symbol,measurement)
    while True:
        time.sleep(60*5)
        trade_data(symbol,measurement)