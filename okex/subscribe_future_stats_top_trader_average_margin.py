import traceback
from multiprocessing import Process
import time
import datetime
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


measurement='okex_future_stats_topTraderAverageMarginUsed'
freqs = [5,15,60]



def topTraderAverageMarginUsed_update(symbol,freq,measurement):
    avg_margin = okexapi.TopTraderAverageMarginUsed(symbol,freq)
    avg_margin_data = avg_margin['data']
    buy_data = avg_margin_data['buydata']
    sell_data = avg_margin_data['selldata']
    time_data = avg_margin_data['timedata']
    
    fields = {}
    for idx in range(len(time_data)):
        fields.update({"long_position":float(buy_data[idx])})
        fields.update({"short_position":float(sell_data[idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":freq})
        dbtime = datetime.datetime.utcfromtimestamp(int(time_data[idx])/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_topTraderAverageMarginUsed(freq,measurement):
    all_tickers = okexapi.all_tickers()
    for symb in all_tickers:
        topTraderAverageMarginUsed_update(symb,freq,measurement)
        time.sleep(0.5)
    while True:
        time.sleep(60*60)
        all_tickers = okexapi.all_tickers()
        for symb in all_tickers:
            try:
                topTraderAverageMarginUsed_update(symb,freq,measurement)
            except Exception:
                error_message = traceback.format_exc()
                #logger(measurement,error_message,symb+":"+freq)
            time.sleep(0.5)

if __name__ == "__main__":
    
    for freq in freqs:
        freq_subscription = Process(target=subscribe_topTraderAverageMarginUsed,args=(freq,measurement))
        freq_subscription.start()
    

