from time import sleep
import traceback
import datetime 
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

def all_symbols():
    contract_info = huobi.contract_info()
    symbols = list(set([d['symbol'] for d in contract_info['data']]))
    return symbols

# index price update
def index_price_update(symbol,measurement):
    index_price = huobi.contract_index_price(symbol)
    index_price_data = index_price['data'][0]
    fields = {}
    fields.update({'index_price':index_price_data['index_price']})
    #fields.update({"index_ts":index_price_data['index_ts']})
    fields.update({"is_api_return_timestamp": True})
    tags = {}
    tags.update({"symbol":symbol})
    dbtime = datetime.datetime.utcfromtimestamp(index_price_data['index_ts']/1000)
    db.write_points_to_measurement(measurement,dbtime,tags,fields)

# subscribe
def subscribe_index_price():
    measurement = "huobidm_index_price"
    symbols = all_symbols()
    for symb in symbols:
        index_price_update(symb,measurement)
    while True:
        sleep(60)
        symbols = all_symbols()
        for symb in symbols:
            try:
                index_price_update(symb,measurement)
            except Exception:
                error_message = traceback.format_exc()
                #logger(measurement,error_message)


if __name__ == "__main__":
    subscribe_index_price()

