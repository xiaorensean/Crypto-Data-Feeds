import traceback
import datetime
import multiprocessing as mp
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
api_dir = os.path.dirname(current_dir) + "/okex/"
sys.path.append(api_dir)
import okex_api.option_stats_api as okexapi

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer


db = Writer()
measurement = "okex_btc_options_implied_volatility_skew"
def write_options_implied_vol_skew(dis):
    measurement = "okex_btc_options_implied_volatility_skew"
    data = okexapi.btc_options_implied_vol_skew(dis)
    for idx in range(len(data['timestamps'])):
        fields = {}
        fields.update({"skew_1m":float(data["heeling"][0][idx])})
        fields.update({"skew_3m": float(data["heeling"][1][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        tags.update({"distance":dis})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_options_implied_vol_skew():
    distances = [0.02, 0.05, 0.10]
    for dis in distances:
        write_options_implied_vol_skew(dis)


if __name__ == "__main__":
    try:
        subscribe_options_implied_vol_skew()
    except:
        error_message = traceback.format_exc()
        #logger(measurement, error_message)
    while True:
        time.sleep(60*58)
        try:
            subscribe_options_implied_vol_skew()
        except:
            error_message = traceback.format_exc()
            #logger(measurement, error_message)
