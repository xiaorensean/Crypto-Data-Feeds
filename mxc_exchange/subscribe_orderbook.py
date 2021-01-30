import traceback
import time
import datetime
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from mxc_api.mxcRestApi import get_orderbook
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "mxc_orderbook"

symbol = "HNS_USDT"

def update_orderbook_data(symbol,data,side_type,ts,mesurement):
    side_data = data[side_type]
    for s in side_data:
        fields = {}
        fields.update({"ref_ts":int(ts)})
        fields.update({"price":float(s['price'])})
        fields.update({"amount":float(s['quantity'])})
        fields.update({"type":side_type})
        fields.update({"is_api_return_timestamp": False})
        dbtime = False
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        
def subscribe_orderbook(symbol,measuremnt):
    try:
        orderbook_data = get_orderbook(symbol)['data']
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,symbol)
        return 
    ts = time.time()*1000
    update_orderbook_data(symbol,orderbook_data,"asks",ts,measurement)
    update_orderbook_data(symbol,orderbook_data,"bids",ts,measurement)

if __name__ == "__main__":
    subscribe_orderbook(symbol,measurement)
    while True:
        time.sleep(20)
        subscribe_orderbook(symbol,measurement)