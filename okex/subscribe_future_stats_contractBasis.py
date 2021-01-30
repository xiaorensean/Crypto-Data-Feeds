import traceback
from multiprocessing import Process
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
api_dir = os.path.dirname(current_dir) + "/okex/"
sys.path.append(api_dir)
import okex_api.futures_indicators_api as okexapi
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer

db = Writer()

all_tickers = okexapi.all_tickers()
contract_type = ['this_week','next_week','quarter','swap']
measurement='okex_future_stats_contractBasis'
freqs = [1,5,60,1440]


def contractBasis_update(freq,symbol,contract_type,measurement):
    data = okexapi.futureBasis(freq,symbol,contract_type)
    
    fields = {}
    for d in data:
        fields.update({"contract_price":float(d[1])})
        fields.update({"contract_index_price":float(d[2])})
        fields.update({"basis":float(d[3])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":freq})
        tags.update({"contract_type":contract_type})
        dbtime = d[0]
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_contractBasis(freq,measurement):
    if freq == 1:
        symbols = okexapi.all_tickers()
        for symb in symbols:
            for ct in contract_type:
                contractBasis_update(freq,symb,ct,measurement)
            time.sleep(0.5)
        while True:
            time.sleep(60*60)
            symbols = okexapi.all_tickers()
            for symb in symbols:
                for ct in contract_type:
                    try:
                        contractBasis_update(freq,symb,ct,measurement)
                    except Exception:
                        tag = symb + ":" + ct
                        error_message = traceback.format_exc()
                        #logger(measurement,error_message,tag)
                    time.sleep(0.5)
    elif freq == 5:
        symbols = okexapi.all_tickers()
        for symb in symbols:
            for ct in contract_type:
                contractBasis_update(freq,symb,ct,measurement)
            time.sleep(0.5)
        while True:
            time.sleep(60*60)
            symbols = okexapi.all_tickers()
            for symb in symbols:
                for ct in contract_type:
                    try:
                        contractBasis_update(freq,symb,ct,measurement)
                    except Exception:
                        tag = symb + ":" + ct
                        error_message = traceback.format_exc()
                        #logger(measurement,error_message,tag)
                    time.sleep(0.5)
    else:
        symbols = okexapi.all_tickers()
        for symb in symbols:
            for ct in contract_type:
                contractBasis_update(freq,symb,ct,measurement)
            time.sleep(0.5)
        while True:
            time.sleep(60*freq)
            symbols = okexapi.all_tickers()
            for symb in symbols:
                for ct in contract_type:
                    try:
                        contractBasis_update(freq,symb,ct,measurement)
                    except Exception:
                        tag = symb + ":" + ct
                        error_message = traceback.format_exc()
                        #logger(measurement,error_message,tag)
                    time.sleep(0.5)

if __name__ == "__main__":
    
    for freq in freqs:
        freq_subscription = Process(target=subscribe_contractBasis,args=(freq,measurement))
        freq_subscription.start()
    





