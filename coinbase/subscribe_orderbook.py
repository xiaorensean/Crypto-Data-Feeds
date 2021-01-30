import traceback
import multiprocessing as mp
import os 
import sys
import time
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from coinbase_api.coinbaseRestApi import get_orderbook

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

tickers = ['KNC-USD','KNC-BTC','BTC-USD','ETH-USD']

measurement = "coinbase_orderbook"

# write bid ask data to influxdb 
def write_bid_ask(ob_type,seq_id,bid_ask,measurement,symbol,current_time):
    fields = {}
    for b in bid_ask:
        fields.update({"ref_ts":int(current_time)})
        fields.update({"price":float(b[0])})
        fields.update({"size":float(b[1])})
        fields.update({"num_orders":float(b[2])})
        fields.update({"sequence":int(seq_id)})
        fields.update({"type":ob_type})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   

    
def write_orderbook(ticker,measurement):
    current_time = time.time() * 1000
    orderbook = get_orderbook(ticker)
    write_bid_ask("ask",orderbook['sequence'],orderbook['asks'],measurement,ticker,current_time)
    write_bid_ask("bid",orderbook['sequence'],orderbook['bids'],measurement,ticker,current_time)
    

def subscribe_orderbook(ticker,measurement):
    try:
        write_orderbook(ticker,measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(20)
        try:
            write_orderbook(ticker,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            time.sleep(1)

if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in tickers:
        orderbook = mp.Process(target=subscribe_orderbook,args=(symb,measurement))
        orderbook.start()
        processes.update({symb:orderbook})
 
