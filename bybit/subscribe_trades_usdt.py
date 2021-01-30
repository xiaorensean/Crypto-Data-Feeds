import traceback
import time
import datetime
import random
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bybit_api.bybitRestApi import trades_usdt, tickers_info
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utils 
db = Writer()
measurement = "bybit_trades"

# writng data to db
def writing_data(data,measurement):
    fields = {}
    for d in data:
        print(d)
        fields.update({"id":int(d["id"])})
        fields.update({"price":str(d["price"])})
        fields.update({"qty":int(d["qty"])})
        fields.update({"side":d["side"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":d["symbol"]})
        dbtime = datetime.datetime.utcfromtimestamp((d['trade_time_ms']+random.random())/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)



# data subscription 
def subscribe_trades(measurement):
    tickers_list = [i for i in [symb['symbol'] for symb in tickers_info()] if "USDT" in i]
    if len(tickers_list) == 0:
        tickers_list = ["BTCUSDT"]
    else:
        tickers_list = tickers_list
    
    for t in tickers_list:
        try:
            data_raw = trades_usdt(t)
            writing_data(data_raw,measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,t)



if __name__ == "__main__":
    subscribe_trades(measurement)
    while True:
        time.sleep(60)
        subscribe_trades(measurement)
