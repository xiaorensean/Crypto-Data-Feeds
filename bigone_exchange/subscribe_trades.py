import multiprocessing as mp
import traceback
import os 
import sys
import time
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bigone_api.bigoneRestApi import get_trades

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


db = InfluxClient()

tickers = ['HNS-BTC','HNS-USDT']

measurement = "bigone_trades"



# write trades data to influxdb 
def write_trades_data(data,symbol,measurement):
    for d in data:
        fields = {}
        fields.update({"amount":float(d["amount"])})
        fields.update({"price":float(d["price"])})
        fields.update({"tradeID":int(d["id"])})
        fields.update({"type":d["taker_side"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = d['inserted_at']
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   

         

def subscribe_trades(ticker,measurement):
    try:
        data = get_trades(ticker)['data']
    except Exception:
        error = traceback.format_exc()
        #logger(measurement, error, ticker)
        data = None
        return 
    if data is not None:
        write_trades_data(data,ticker,measurement)
    else:
        pass
    while True:
        time.sleep(30)
        try:
            data = get_trades(ticker)['data']
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error, ticker)
            data = None
            return 
        if data is not None:
            write_trades_data(data,ticker,measurement)
        else:
            pass
        

if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in tickers:
        trades = mp.Process(target=subscribe_trades,args=(symb,measurement))
        trades.start()
        processes.update({symb:trades})
