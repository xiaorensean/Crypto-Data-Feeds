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


measurement='okex_future_stats_FutureTakerBuySell'
freqs = [5,60,24]


def future_taker_buy_sell_update(symbol,freq,measurement):
    futures_taker_buy_sell = okexapi.FuturesTakerBuyAndSell(symbol,freq)
    futures_taker_buy_sell_data = futures_taker_buy_sell['data']
    buy_volume = futures_taker_buy_sell_data['buyVolumes']
    sell_volume = futures_taker_buy_sell_data['sellVolumes']
    timestamps = futures_taker_buy_sell_data['timestamps']

    fields = {}
    for idx in range(len(timestamps)):
        fields.update({"buy_volume":int(buy_volume[idx])})
        fields.update({"sell_volume":int(sell_volume[idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":freq})
        dbtime = datetime.datetime.utcfromtimestamp(timestamps[idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_futureTakerBuySell(freq,measurement):
    if freq == 24:
        all_tickers = okexapi.all_tickers()
        for symb in all_tickers:
            future_taker_buy_sell_update(symb,freq,measurement)
            time.sleep(0.5)
        while True:
            time.sleep(60*60*24)
            all_tickers = okexapi.all_tickers()
            for symb in all_tickers:
                try:
                    future_taker_buy_sell_update(symb,freq,measurement)
                except Exception:
                    error_message = traceback.format_exc()
                    #logger(measurement,error_message,symb+":"+freq)
                time.sleep(0.5)
    else:
        time.sleep(10)
        all_tickers = okexapi.all_tickers()
        for symb in all_tickers:
            future_taker_buy_sell_update(symb,freq,measurement)
            time.sleep(0.5)
        while True:
            time.sleep(60*65)
            all_tickers = okexapi.all_tickers()
            for symb in all_tickers:
                try:
                    future_taker_buy_sell_update(symb,freq,measurement)
                except Exception:
                    error_message = traceback.format_exc()
                    #logger(measurement,error_message,symb+":"+freq)
                time.sleep(0.5)


if __name__ == "__main__":
    
    for freq in freqs:
        freq_subscription = Process(target=subscribe_futureTakerBuySell,args=(freq,measurement))
        freq_subscription.start()
    