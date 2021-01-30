import traceback
import multiprocessing
#import nest_asyncio
import time

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata
from deribit_api.deribitRestApi import RestClient
sys.path.append(os.path.dirname( current_dir ))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


deribit = RestClient()

db = Writer()


def symbol_eth_cluster(num):
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    btc_symbols = [symb for symb in symbols  if "ETH" in symb]
    clusters = str(len(btc_symbols)/num)
    integ = int(clusters.split(".")[0])
    ss = []
    for i in range(integ+1):
        s = btc_symbols[num*i:num*(i+1)]
        ss.append(s)
    return ss


def symbol_eth():
    symbols = [i['instrumentName'] for i in deribit.getinstruments()]
    eth_symbols = [symb for symb in symbols  if "ETH" in symb]
    return eth_symbols

measurement = "deribit_orderbook"


def write_ask_bid(data,side_type,measurement):
    fields = {}
    side_data = data[side_type]
    timestamp = data['tstamp']
    instrument_name = data['instrument']
    for sd in side_data:
        fields.update({"price":float(sd['price'])})
        fields.update({"amount":float(sd['amount'])})
        fields.update({"timestamp":timestamp})
        fields.update({"type":side_type})
        tags = {"symbol":instrument_name}
        time = False
        db.write_points_to_measurement(measurement,time,tags,fields)


def subscribe_orderbook_op(ticker,measurement):
    try:
        data = deribit.getorderbook(ticker,200)
        print(data)
    except Exception:
        error = traceback.format_exc()
        logger(measurement, error,ticker)
        data = None
        pass
    if data is not None:
        print("Writting ", ticker)
        write_metadata(data)
    else:
        print(ticker," not avaiable")
    while True:
        time.sleep(20)
        try:
            data = deribit.getorderbook(ticker,200)
            print(data)
        except Exception:
            error = traceback.format_exc()
            logger(measurement,error,ticker)
            data = None
            pass
        if data is not None:
            print("Writting ", ticker)
            write_metadata(data)
        else:
            print(ticker," not avaiable")
    
        
def write_metadata(data):
    # subscribe option orderbook data
    measurement = "deribit_orderbook"
    write_ask_bid(data,"asks",measurement)
    write_ask_bid(data,"bids",measurement)


def subscribe_orderbook():
    num = 25
    try:
        symbols = symbol_eth_cluster(num)[15]
    except:
        return
    for symb in symbols:
        print(symb)
        try:
            data = deribit.getorderbook(symb,200)
            write_metadata(data)
        except Exception:
            error = traceback.format_exc()
            logger("deribit_orderbook",error,symb)


if __name__ == "__main__":
    subscribe_orderbook()
    while True:
        time.sleep(55)
        subscribe_orderbook()



