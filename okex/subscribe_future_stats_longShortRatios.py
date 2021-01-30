import traceback
from datetime import datetime
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

measurement='okex_future_stats_longShortPositionRatio'
freqs = [5,60,1440]


def longShortPositionRatio_update(symbol,freq,measurement,init=None):
    data = okexapi.longShortPositionRatio(symbol,freq)
    ratios = data['ratios']
    timestamp = [datetime.fromtimestamp(ts/1000) for ts in data['timestamps']] 
    
    if init is not None:
        fields= {}
        for idx in range(len(ratios)):
            fields.update({'ratios':float(ratios[idx])})
            fields.update({"is_api_return_timestamp": True})
            dbtime = timestamp[idx]
            tags = {}
            tags.update({"symbol":symbol})
            tags.update({"frequency":freq})
            db.write_points_to_measurement(measurement,dbtime,tags,fields)
    else:
        fields= {}
        fields.update({'ratios':float(ratios[-1])}) 
        time = timestamp[-1]
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":freq})
        db.write_points_to_measurement(measurement,time,tags,fields)


def subscribe_longShortPositionRatio(freq,measurement):
    symbols = okexapi.all_tickers()
    for symb in symbols:
        longShortPositionRatio_update(symb,freq,measurement,"all")
    while True:
        time.sleep(60*freq)
        symbols = okexapi.all_tickers()
        for symb in symbols:
            try:
                longShortPositionRatio_update(symb,freq,measurement,"all")
            except:
                time.sleep(10)
                try:
                    longShortPositionRatio_update(symb,freq,measurement,"all")
                except Exception:
                    error_message = traceback.format_exc()
                    #logger(measurement,error_message,symb+":"+freq)




if __name__ == "__main__":
    
    for freq in freqs:
        freq_subscription = Process(target=subscribe_longShortPositionRatio,args=(freq,measurement))
        freq_subscription.start()
    
    

