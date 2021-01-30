import traceback
import time
import datetime
import random
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bybit_api.bybitRestApi import trades, tickers_info
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
        fields.update({"id":d["id"]})
        fields.update({"price":str(d["price"])})
        fields.update({"qty":d["qty"]})
        fields.update({"side":d["side"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":d["symbol"]})
        dbtime = datetime.datetime.utcfromtimestamp((float(d['current_snapshot'])+random.random())/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

  
    
# data subscription 
def subscribe_trades(measurement):
    ticker_list = [symb['symbol'] for symb in tickers_info()]
    if len(ticker_list) == 0:
        ticker_list = ["BTCUSD","ETHUSD","EOSUSD","XRPUSD"]
    else:
        ticker_list = ticker_list
    for t in ticker_list:
        try:
            data_raw = trades(t)
            if data_raw is None:
                pass
            else:
                writing_data(data_raw,measurement)
        except Exception:
            error_msg = traceback.format_exc()
            #logger(measurement,error_msg,t)
            

if __name__ == "__main__":
    subscribe_trades(measurement)
    while True:
        time.sleep(60)
        subscribe_trades(measurement)
