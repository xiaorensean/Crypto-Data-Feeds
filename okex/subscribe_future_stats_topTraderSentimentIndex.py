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


measurement='okex_future_stats_topTraderSentimentIndex'
freqs = [5,15,60]

symbol = "BTC"
freq = 60

def topTraderSentimentIndex_update(symbol,freq,measurement):
    senti_index = okexapi.TopTraderSentimentalIndex(symbol,freq)
    senti_index_data = senti_index['data']
    buy_data = senti_index_data['buydata']
    sell_data = senti_index_data['selldata']
    time_data = senti_index_data['timedata']

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


def subscribe_topTraderSentimentIndex(freq,measurement):
    all_tickers = okexapi.all_tickers()
    for symb in all_tickers:
        topTraderSentimentIndex_update(symb,freq,measurement)
        time.sleep(0.5)
    while True:
        time.sleep(60*60)
        all_tickers = okexapi.all_tickers()
        for symb in all_tickers:
            try:
                topTraderSentimentIndex_update(symb,freq,measurement)
            except Exception:
                error_message = traceback.format_exc()
                #logger(measurement,error_message,symb+":"+freq)
            time.sleep(0.5)

if __name__ == "__main__":
    
    for freq in freqs:
        freq_subscription = Process(target=subscribe_topTraderSentimentIndex,args=(freq,measurement))
        freq_subscription.start()
    

