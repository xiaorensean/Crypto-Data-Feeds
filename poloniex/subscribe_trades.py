import traceback
import multiprocessing as mp
import numpy as np
import time
import random
import calendar
import os
import datetime
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from poloninex_api.poloniexRestApi import returnTradeHistory
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient


db = InfluxClient()

measurement = 'poloniex_trades'




symbols = ["USDT_SNX","TRX_SNX","BTC_SNX"]

# write trades data to influxdb 
def write_trades_data(data,symbol,measurement):
    for d in data:
        fields = {}
        fields.update({"amount":float(d["amount"])})
        fields.update({"total":float(d["total"])})
        fields.update({"price":float(d["rate"])})
        fields.update({"tradeID":int(d["tradeID"])})
        fields.update({"globalTradeID":int(d["globalTradeID"])})
        fields.update({"orderNumber":int(d["orderNumber"])})
        fields.update({"type":d["type"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        dt_temp = datetime.datetime.strptime(d['date'], "%Y-%m-%d %H:%M:%S")
        uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
        dt = datetime.datetime.utcfromtimestamp(uts / 1000)
        dbtime = dt
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   
        
    
# historical trades data 
def write_hist_trades(symbol,measurement):
    data = returnTradeHistory(symbol)
    last_trades_ts = calendar.timegm(datetime.datetime.strptime(data[-1]['date'], "%Y-%m-%d %H:%M:%S").timetuple())
    get_all_data = False
    all_data = []
    while not get_all_data:
        data_temp = returnTradeHistory(symbols[0],int(last_trades_ts)-60*60,int(last_trades_ts))[1:]
        write_trades_data(data_temp, symbol, measurement)
        last_trades_ts = calendar.timegm(datetime.datetime.strptime(data_temp[-1]['date'], "%Y-%m-%d %H:%M:%S").timetuple())
        if data_temp[-1]['tradeID'] == 1:
            get_all_data = True
        else:
            pass

# live trades data
def write_trades_update(symbol,measurement):
    #try:
    #    last_trades_date = db.query_tables(measurement, ["*","order by time desc limit 1"])['date'].tolist()[0]
    #except IndexError:
    #    write_hist_trades(symbol,measurement)
    #    return
    #last_trades_ts = calendar.timegm(datetime.datetime.strptime(last_trades_date, "%Y-%m-%d %H:%M:%S").timetuple())
    current_ts = int(np.ceil(time.time()))
    data = returnTradeHistory(symbol)
    write_trades_data(data, symbol, measurement)


def subscribe_trades(symbol,measurement):
    try:
        write_trades_update(symbol,measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(60*10)
        try:
            write_trades_update(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)

if __name__ == "__main__":
    for symb in symbols:
        collecting_symb = mp.Process(target=subscribe_trades, args=(symb,measurement))
        collecting_symb.start()


