from time import sleep
import multiprocessing as mp
import traceback
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
    contract_info = huobi.get_swap_info()
    symbols = []
    for d in contract_info['data']:
       symbols.append(d['contract_code'])
    return symbols 


def one_side_data_update(tick,side_type,measurement,symbol):
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
        tags.update({"contract_code":symbol})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_orderbook_data(symbol,measurement):
    orderbook = huobi.get_swap_depth(symbol)
    tick = orderbook['tick']
    one_side_data_update(tick,'ask',measurement,symbol)
    one_side_data_update(tick,'bid',measurement,symbol)

        
def subscribe_orderbook_swap(symbol,measurement):
    sleep(30)
    write_orderbook_data(symbol,measurement)
    while True:
        sleep(20)
        try:
            write_orderbook_data(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol)
            
if __name__ == "__main__":
    symbols = get_valid_symbol()
    processes = {}
    for symb in symbols:
        orderbook = mp.Process(target=subscribe_orderbook_swap,args=(symb,measurement))
        orderbook.start()
        processes.update({symb:orderbook})

        
