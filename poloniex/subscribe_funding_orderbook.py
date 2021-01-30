import traceback
import multiprocessing as mp
import time
import os
import datetime
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from poloninex_api.poloniexRestApi import returnCurrencies,returnLoanOrders
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient


influxdb = InfluxClient()
measurement = 'poloniex_funding_orderbook'


currency = returnCurrencies()

def currency_filter(currency):
    existing_currency = []
    for symb in currency.keys():
        funding_ob = returnLoanOrders(symb)
        if len(funding_ob['offers']) == 0 and len(funding_ob['demands']) == 0:
            pass
        else:
            existing_currency.append(symb)
    return existing_currency


def write_data(itype,symbol,funding_orderbook):
    try:
        data = funding_orderbook[itype]
        print(data)
    except:
        time.sleep(30)
        try:
            data = funding_orderbook[itype]
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement, error_message,symbol)
            return    
    timestamp = time.time() * 1000
    if len(data) != 0:
        for fob in data:
            fields = {}
            fields.update({"amount":float(fob["amount"])})
            fields.update({"rate":float(fob["rate"])})
            fields.update({"rangeMax":int(fob["rangeMax"])})
            fields.update({"rangeMin":int(fob["rangeMin"])})
            fields.update({"type":itype})
            fields.update({"ref_ts":int(timestamp)})
            fields.update({"is_api_return_timestamp": False})
            times = False
            tags = {}
            tags.update({"symbol":symbol})
            influxdb.write_points_to_measurement(measurement,times,tags,fields)
    else:
        pass

def asks_demands(symbol):
    funding_orderbook = returnLoanOrders(symbol)
    itype = "offers" 
    write_data(itype,symbol,funding_orderbook)
    itype = "demands"
    write_data(itype,symbol,funding_orderbook)


def collecting(symbol):
    asks_demands(symbol)
    while True:
        time.sleep(60)
        asks_demands(symbol)


currency_in_loop = currency_filter(currency)

for symb in currency_in_loop:
    collecting_symb = mp.Process(target=collecting, args=(symb,))
    collecting_symb.start()


