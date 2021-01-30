import multiprocessing as mp
import traceback
import random
import os 
import sys
import datetime
import time
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bigone_api.bigoneRestApi import get_orderbook

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

tickers = ['HNS-BTC','HNS-USDT']

measurement = "bigone_orderbook"


# write bid ask data to influxdb 
def write_bid_ask(ob_type,bid_ask,measurement,symbol,timestamp):
    fields = {}
    for b in bid_ask:
        fields.update({"ref_ts":int(timestamp)})
        fields.update({"price":float(b['price'])})
        fields.update({"size":float(b['quantity'])})
        fields.update({"order_count":b['order_count']})
        fields.update({"type":ob_type})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   

    
def write_orderbook(ticker,measurement):
    current_ts = time.time()*1000
    orderbook = get_orderbook(ticker)['data']
    write_bid_ask("ask",orderbook['asks'],measurement,ticker, current_ts)
    write_bid_ask("bid",orderbook['bids'],measurement,ticker, current_ts)
    

def subscribe_orderbook(ticker,measurement):
    try:
        write_orderbook(ticker,measurement)
    except Exception:
        error = traceback.format_exc()
        #logger(measurement, error,ticker)
    while True:
        time.sleep(20)
        try:
            write_orderbook(ticker,measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,ticker)
            time.sleep(1)


if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in tickers:
        orderbook = mp.Process(target=subscribe_orderbook,args=(symb,measurement))
        orderbook.start()
        processes.update({symb:orderbook})

