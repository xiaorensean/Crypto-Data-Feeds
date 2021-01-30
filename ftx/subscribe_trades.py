import traceback
import datetime
import time
import random
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
from ftx_api.FtxRest import get_all_tickers, get_trades
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient
import multiprocessing as mp




db = InfluxClient()

measurement = "ftx_trades"

def write_trades_data(data,symbol,measurement):
    for t in data:
        fields = {i:t[i] for i in t if i != "time"}
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        ts = " ".join(t['time'].split("+")[0].split("T"))
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
        dt = datetime.datetime.utcfromtimestamp(uts / 1000)
        dbtime = dt
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def write_hist_data(ticker,measurement):
    end_time = datetime.datetime.now().timestamp()
    get_all_data = False
    while not get_all_data:
        start_time = end_time - 60*60*5
        print(start_time)
        trades_data = get_trades(ticker, start_time, end_time)
        if len(trades_data) != 0:
            write_trades_data(trades_data,ticker,measurement)
            end_time = start_time
            dt = trades_data[-1]['time']
            start_time = time.mktime(datetime.datetime.strptime(dt.split("+")[0].replace("T"," "), "%Y-%m-%d %H:%M:%S.%f").timetuple())
        else:
            get_all_data = True
            break

def write_update_data(ticker,measurement):
    end_time = datetime.datetime.now().timestamp()
    start_time = end_time - 60*60*10
    trades_data = get_trades(ticker, start_time, end_time)
    print(trades_data)
    if len(trades_data) != 0:
        write_trades_data(trades_data,ticker,measurement)
    else:
        return 


def subscribe_trades(measurement):
    all_tickers = get_all_tickers()
    for symb in all_tickers:
        try:
            write_update_data(symb,measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement,error,symb)
    while True:
        time.sleep(60*10)
        for symb in all_tickers:
            try:
                write_update_data(symb,measurement)
            except Exception:
                error = traceback.format_exc()
    #            logger(measurement,error,symb)

if __name__ == '__main__':
    subscribe_trades( measurement)


