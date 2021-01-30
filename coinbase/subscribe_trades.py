import traceback
import multiprocessing as mp
import os 
import sys
import time
import datetime
import random
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from coinbase_api.coinbaseRestApi import get_trades, get_tickers

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

tickers = [i['id'] for i in get_tickers() if i['id'][-4:] == "-USD"] + ['KNC-BTC']

measurement = "coinbase_trades"


# write bid ask data to influxdb 
def write_trades_data(symbol,measurement):
    trades = get_trades(symbol)
    fields = {}
    for d in trades:
        fields.update({"price":float(d["price"])})
        fields.update({"size":float(d["size"])})
        fields.update({"trade_id":int(d["trade_id"])})
        fields.update({"side":d["side"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        # add unique timestamp index
        ts = " ".join(d['time'].split("Z")[0].split("T"))
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
        dbtime = datetime.datetime.utcfromtimestamp(uts / 1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   



def subscribe_trades(ticker,measurement):
    try:
        write_trades_data(ticker,measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(60*4)
        try:
            write_trades_data(ticker,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            time.sleep(1)



if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in tickers:
        trades = mp.Process(target=subscribe_trades,args=(symb,measurement))
        trades.start()
        processes.update({symb:trades})

