import traceback
from datetime import datetime
import multiprocessing as mp
import time
import numpy as np
import copy
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from utils import get_all_tickers
from bitfinex_api.BfxRest import BITFINEXCLIENT

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


# utils
def get_date(ts):
    return datetime.utcfromtimestamp(ts/1000)

measurement = 'bitfinex_funding_orderbook'


# bitfinex client
API_KEY = "API_KEY"
API_SECRETE = "API_SECRETE"
bitfinex =  BITFINEXCLIENT(API_KEY,API_SECRETE)
influxdb = InfluxClient()



def subscribe_funding_orderbook(symbol):
    funding_orderbook_raw = bitfinex.get_public_books(symbol)
    current_ts = time.time() * 1000
    funding_orderbook = [ob[:-1] + [np.abs(ob[-1])] + ['ask'] + [current_ts] if ob[-1] > 0 else ob[:-1] + [np.abs(ob[-1])] + ['bid'] + [current_ts] for ob in funding_orderbook_raw]
    if funding_orderbook is None:
        return
    # wrtie to database
    for t in funding_orderbook:
        data_entry = {'RATE':None,'PERIOD':None,'COUNT':None,'AMOUNT':None,'TYPE':None,'ref_ts':None}
        for idx in range(len(t)):
            keys = list(data_entry.keys())
            if keys[idx] == 'RATE':
                data_entry[keys[idx]] = float(t[idx])
            if keys[idx] == 'PERIOD':
                data_entry[keys[idx]] = float(t[idx])
            if keys[idx] == 'COUNT':
                data_entry[keys[idx]] = int(t[idx])
            if keys[idx] == 'AMOUNT':
                data_entry[keys[idx]] = float(t[idx])
            if keys[idx] == 'TYPE':
                data_entry[keys[idx]] = str(t[idx])
            if keys[idx] == 'ref_ts':
                data_entry[keys[idx]] = int(t[idx])
        times = False
        tags = {}
        tags.update({'SYMBOL':symbol})
        fields = copy.copy(data_entry)
        fields.update({"is_api_return_timestamp": False})
        influxdb.write_points_to_measurement(measurement,times,tags,fields)    
    

def collecting(symb):
    try:
        subscribe_funding_orderbook(symb)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message,symb)
    while True:
        time.sleep(60)
        try:
            subscribe_funding_orderbook(symb)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symb)


# collect hsitory funding trades
funding_tickers = get_all_tickers('funding') 

for symb in funding_tickers:
    collecting_symb = mp.Process(target=collecting, args=(symb,))
    collecting_symb.start()
 








