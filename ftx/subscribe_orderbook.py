import traceback
import numpy as np
import time
import multiprocessing as mp
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from ftx_api.FtxRest import get_contract_names, get_orderbook
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


# utility
db = InfluxClient()

measurement = "ftx_orderbook"


def check_new_tickers():
    tickers = get_contract_names()
    tags = db.get_tag_values(measurement,"symbol")
    new_tickers = []
    for t in tickers:
        if t in tags:
            pass
        else:
            new_tickers.append(t)
    return new_tickers


# get only move contracts
def get_move_contract_name():
    future_contracts = get_contract_names()
    move_contracts = [fc for fc in future_contracts if "MOVE" in fc]
    return move_contracts

def ticker_batch(tickers_batch,all_contracts):
    total_tickers = len(all_contracts)
    number_batch = int(np.ceil(total_tickers/tickers_batch))
    tickers_per_batch = []
    for idx in range(number_batch):
        tickers_per_batch.append(all_contracts[0+idx*tickers_batch:tickers_batch+idx*tickers_batch])
    return tickers_per_batch


# write bid ask data to influxdb 
def write_bid_ask(ob_type,bid_ask,measurement,symbol,current_time):
    fields = {}
    for b in bid_ask:
        fields.update({"ref_ts":int(current_time)})
        fields.update({"price":b[0]})
        fields.update({"size":b[1]})
        fields.update({"type":ob_type})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   


# subscribe orderbook data
def write_orderbook_data(symb,measurement):
    current_time = time.time()*1000
    orderbook = get_orderbook(symb)
    bids = orderbook['bids']
    asks = orderbook['asks']
    write_bid_ask("ask",asks,measurement,symb,current_time)
    write_bid_ask("bid",bids,measurement,symb,current_time)


def subscribe_orderbook_batch():  
    orderbooks = []
    all_contracts = get_contract_names()
    for mc in all_contracts:
        current_time = str(time.time())
        orderbook = get_orderbook("ADA-0327")
        orderbook.update({"timestamp":current_time})
        orderbooks.append(orderbook)
    return orderbooks


def subscribe_orderbook(symb,measurement):
    try:
        write_orderbook_data(symb,measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)
        time.sleep(30)
        write_orderbook_data(symb,measurement)
    while True:
        # set up scheduler for 30 seconds
        time.sleep(27)
        try:
            write_orderbook_data(symb,measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message)
            time.sleep(27)
            write_orderbook_data(symb,measurement)


if __name__ == "__main__":
    tickers = get_contract_names()
    processes = {}
    for symb in tickers:
        orderbook = mp.Process(target=subscribe_orderbook,args=(symb,measurement))
        orderbook.start()
        processes.update({symb:orderbook})

    

