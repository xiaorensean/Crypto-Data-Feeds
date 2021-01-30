from time import sleep
import multiprocessing as mp
import traceback
import time
import os
import sys
 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))
import huobi_api.HbRest as huobi

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer

db = Writer()

measurement = "huobidm_orderbook"

def get_valid_symbol():
    contract_info = huobi.contract_info()
    symbols = {}
    for d in contract_info['data']:
        if d["contract_type"] == "this_week": 
            symbols.update({d['symbol']+"_CW":d['contract_code']})
        elif d["contract_type"] == "next_week":
            symbols.update({d['symbol']+"_NW":d['contract_code']})
        else:
            symbols.update({d['symbol']+"_CQ":d['contract_code']})
    return symbols 


def one_side_data_update(tick,side_type,measurement,symbol_data):
    if side_type == "ask":
        side_data = tick['asks']
    elif side_type == "bid":
        side_data = tick['bids']
    else:
        pass
    fields = {}
    for sd in side_data:
        fields.update({"price":float(sd[0])})
        fields.update({"vol":float(sd[1])})
        fields.update({"type":side_type})
        fields.update({"ref_ts":int(tick['ts'])})
        fields.update({"id":tick['mrid']})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"contract_code":symbol_data})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_orderbook_data(symbol_api,symbol_data,measurement):
    orderbook = huobi.market_depth(symbol_api, "step0")
    tick = orderbook['tick']
    one_side_data_update(tick,'ask',measurement,symbol_data)
    one_side_data_update(tick,'bid',measurement,symbol_data)


def write_orderbook_futures(symbol_api,symbol_data,measurement):
    sleep(30)
    write_orderbook_data(symbol_api,symbol_data,measurement)
    while True:
        sleep(20)
        try:
            write_orderbook_data(symbol_api,symbol_data,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol_data)
            
def get_orderbook_data_tickers():
    symbols = get_valid_symbol()
    processes = {}
    for symb in symbols:
        orderbook = mp.Process(target=write_orderbook_futures,args=(symb,symbols[symb],measurement))
        orderbook.start()
        processes.update({symb:orderbook})
    return processes

def subscribe_orderbook_future():
    processes = get_orderbook_data_tickers()
    while True:
        time.sleep(60*60)
        # Check for each time the tickers are rolling on 
        # Shut down all existing processes before starting new around
        for symbol in processes:
            processes[symbol].join()
        processes = get_orderbook_data_tickers()

    
if __name__ == "__main__":
    subscribe_orderbook_future()

        
