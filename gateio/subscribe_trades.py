import traceback
import multiprocessing as mp
import random
import os 
import sys
import datetime
import time
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from gateio_api.gateioRestApi import get_trades

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger
db = InfluxClient()

tickers = ['hns_usdt','hns_btc']

measurement = "gateio_trades"


# write trades data to influxdb 
def write_trades_data(data,symbol,measurement):
    for d in data:
        fields = {}
        fields.update({"amount":float(d["amount"])})
        fields.update({"price":float(d["rate"])})
        fields.update({"total":float(d["total"])})
        fields.update({"tradeID":int(d["tradeID"])})
        fields.update({"type":d["type"]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = datetime.datetime.utcfromtimestamp((int(d["timestamp"])*1000+random.random())/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   


def get_trades_data_tid(tid,symbol,measurement):
    get_all_trades = False
    all_trades = []
    while not get_all_trades:
        trades = get_trades(symbol,tid)['data']
        if len(trades) == 0:
            get_all_trades = True
        else:
            tid = trades[-1]['tradeID']
            all_trades += trades
    write_trades_data(all_trades,symbol,measurement)
         

def subscribe_trades(ticker,measurement):
    #try:
    #    tid_data = db.query_tables('gateio_trades',['max(tradeID)',""])
    #    tid = tid_data['max'].tolist()[0]
    #except IndexError:
    #    get_trades_data_tid(1,ticker,measurement)
    #    return 
    try:
        #get_trades_data_tid(trades_data,ticker,measurement)
        trades_data = get_trades(ticker)['data']
        write_trades_data(trades_data,ticker,measurement)
    except Exception: 
        error = traceback.format_exc()
        logger(measurement,error,ticker)
    while True:
        time.sleep(60*30)
        #try:
        #    tid_data = db.query_tables('gateio_trades',['max(tradeID)',""])
        #    tid = tid_data['max'].tolist()[0]
        #except IndexError:
        #    get_trades_data_tid(1,ticker,measurement)
        #    return 
        try:
            #get_trades_data_tid(trades_data,ticker,measurement)
            trades_data = get_trades(ticker)['data']
            write_trades_data(trades_data,ticker,measurement)
        except Exception: 
            error = traceback.format_exc()
            logger(measurement,error,ticker)


if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in tickers:
        trades = mp.Process(target=subscribe_trades,args=(symb,measurement))
        trades.start()
        processes.update({symb:trades})
