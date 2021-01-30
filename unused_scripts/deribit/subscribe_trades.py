import traceback
import datetime
import random
import time
import os 
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from deribit_api.deribitRestApi import RestClient
import multiprocessing
import json
from time import sleep

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


db = InfluxClient()
deribit = RestClient()
measurement = "deribit_trades"

def symbol_btc_cluster(num):
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    btc_symbols = [symb for symb in symbols  if "BTC" in symb]
    clusters = str(len(btc_symbols)/num)
    integ = int(clusters.split(".")[0])
    ss = []
    for i in range(integ+1):
        s = btc_symbols[num*i:num*(i+1)]
        ss.append(s)
    return ss

def symbol_btc():
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    btc_symbols = [symb for symb in symbols  if "BTC" in symb]
    return btc_symbols



def subscribe_trades(measurement):
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    for s in symbols:
        print(s)
        try:
            data = deribit.getlasttrades(s,100)
            time.sleep(0.01)
            write_trade_data(measurement, data)
        except Exception as err:
            print(err)
            #error = traceback.format_exc()
            #logger(measurement,error,s)


def write_trade_data(measurement,data):
    for d in data:
        print(d)
        fields = {}
        fields.update({"amount":float(d["amount"])})
        fields.update({"index_price":float(d["indexPrice"])})
        try:
            fields.update({"iv":float(d["iv"])})
        except: 
            fields.update({"iv":None})
        fields.update({"price":float(d["price"])})
        fields.update({"amount":float(d["quantity"])})
        fields.update({"tick_direction":int(d["tickDirection"])})
        fields.update({"trade_id":int(d["tradeId"])})
        fields.update({"trade_seq":int(d["tradeSeq"])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":d["instrument"]})
        tags.update({"side":d["direction"]})
        dbtime = datetime.datetime.utcfromtimestamp((d['timeStamp'] + random.random())/1000)
        db.write_points_to_measurement(measurement, dbtime, tags, fields)


if __name__ == "__main__":
    subscribe_trades(measurement)
    while True:
        time.sleep(60*60*10)
        subscribe_trades(measurement)



