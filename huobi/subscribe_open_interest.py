import traceback
import time
import datetime
import os
import sys
 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))
import huobi_api.HbRest as huobi

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()
measurement = "huobidm_open_interest"

def open_interest_swap():
    symbols = [i['contract_code'] for i in huobi.get_swap_info()['data']]
    data = []
    for symb in symbols:
        oi_data_swap = huobi.get_swap_open_interest(symb)['data']
        data += oi_data_swap    
    data_new = [d.update({"contract_type":"swap"}) for d in data]
    return data


def subscribe_open_interest():
    oi_data_futures = huobi.contract_open_interest()
    ts = datetime.datetime.utcfromtimestamp(oi_data_futures['ts']/1000)
    data = oi_data_futures['data']
    oi_data_swap = open_interest_swap()
    data += oi_data_swap

    for data_entry in data:
        fields = {}
        fields.update({'amount':float(data_entry['amount'])})
        fields.update({'volume':float(data_entry['volume'])})
        fields.update({'symbol':data_entry['symbol']})
        fields.update({"contract_type":data_entry['contract_type']})
        fields.update({"is_api_return_timestamp": True})
        dbtime = ts
        tags = {}
        tags.update({"contract_code":data_entry['contract_code']})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        

subscribe_open_interest()
while True:
    time.sleep(60)
    try:
        subscribe_open_interest()
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
 
        
