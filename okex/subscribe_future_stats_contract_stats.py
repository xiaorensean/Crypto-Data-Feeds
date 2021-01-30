import traceback
import datetime
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



freqs = [5,60,24]

db = Writer()

def swap_data_updating(measurement,symbol,freq):
    swap_symbol = symbol + "-USD-SWAP"    
    swap = okexapi.SwapFundingRates(swap_symbol,freq)
    swap_data = swap['data']
    timestamp = swap_data['timestamp'] 
    fundingrates = swap_data['fundingRate']
    fields = {}
    for idx in range(len(timestamp)):
        fields.update({'funding_rate':float(fundingrates[idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"frequency":freq})
        tags.update({"symbol":symbol})
        dbtime = datetime.datetime.utcfromtimestamp(timestamp[idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
    

def vol_oi_updating(measurement,symbol,freq):
    oi_volume = okexapi.FuturesOpenInterestAndTradingVolume(symbol,freq)
    oi_volume_data = oi_volume['data']
    oi_data = oi_volume_data['openInterests']
    vol_data = oi_volume_data['volumes']
    ts = oi_volume_data['timestamps']
    fields = {}
    for idx in range(len(ts)):
        fields.update({"open_interests":int(oi_data[idx])})
        fields.update({"volumes":int(vol_data[idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"frequency":freq})
        tags.update({"symbol":symbol})
        dbtime = datetime.datetime.utcfromtimestamp(ts[idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
   

def contract_data_updating(freq):
    all_tickers = okexapi.all_tickers()
    for symb in all_tickers:
        measurement = "okex_swap_stats_FundingRate"
        try:
            swap_data_updating(measurement,symb,freq)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,freq)
        time.sleep(1)
        measurement = "okex_future_stats_OpenInterestVolume"
        try:
            vol_oi_updating(measurement,symb,freq)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,freq)
            
def subscribe_contract_data(freq):
    if freq == 24:
        contract_data_updating(24)
        while True:
            time.sleep(60*60*24)
            contract_data_updating(24)
    else:
        contract_data_updating(freq)
        while True:
            time.sleep(60*freq)
            contract_data_updating(freq)

     
if __name__ == "__main__":
    for freq in freqs:
        freq_subscription = Process(target=subscribe_contract_data,args=(freq,))
        freq_subscription.start()
    