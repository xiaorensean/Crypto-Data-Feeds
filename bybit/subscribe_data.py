import traceback
import time
import datetime
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bybit_api.bybitRestApi import tickers_info
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utils 
db = Writer()


# writng data to db
def writing_data(data):
    measurement = "bybit_tickers"
    for d in data:
        fields = {i:float(d[i]) for i in d if i != "current_snapshot" and i != "symbol" and i != "last_tick_direction" and i != "next_funding_time"}
        fields.update({"last_tick_direction":d["last_tick_direction"]})
        fields.update({"next_funding_time":d["next_funding_time"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":d["symbol"]})
        dbtime = datetime.datetime.utcfromtimestamp(float(data[0]['current_snapshot']))
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


# a entire process
def subscribe_tickers_info():
    data = tickers_info()
    writing_data(data)
    

if __name__ == "__main__":
    try:
        subscribe_tickers_info()
    except Exception:
        error_msg = traceback.format_exc()
        #logger("bybit_tickers",error_msg)
    while True:
        time.sleep(60)
        try:
            subscribe_tickers_info()
        except Exception:
            error_msg = traceback.format_exc()
            #logger("bybit_tickers",error_msg)


