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
measurement = "okex_btc_options_implied_volatility"
def write_options_implied_vol(delivery_time):
    measurement = "okex_btc_options_implied_volatility"
    data = okexapi.btc_options_implied_vol(delivery_time,symbol='BTC-USD')
    current_ts = int(time.time())
    for idx in range(len(data['call_ask_vol'])):
        fields = {}
        fields.update({"call_ask_vol":float(data["call_ask_vol"][idx])})
        fields.update({"call_bid_vol": float(data["call_bid_vol"][idx])})
        fields.update({"put_ask_vol":float(data["put_ask_vol"][idx])})
        fields.update({"put_bid_vol": float(data["put_bid_vol"][idx])})
        fields.update({"mark_vol": float(data["mark_vol"][idx])})
        fields.update({"ref_ts":current_ts})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"strike":data["strike"][idx]})
        tags.update({"delivery_time":delivery_time})
        tags.update({"symbol":"BTCUSD"})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_options_implied_vol():
    delivery_times = okexapi.btc_options_oi_tradingVol_expiry(8*60*60, symbol="BTC-USD")['delivery_time']
    for dt in delivery_times:
        write_options_implied_vol(dt)


if __name__ == "__main__":
    try:
        subscribe_options_implied_vol()
    except:
        error_message = traceback.format_exc()
        #logger(measurement, error_message)
    while True:
        time.sleep(60*58)
        try:
            subscribe_options_implied_vol()
        except:
            error_message = traceback.format_exc()
            #logger(measurement, error_message)
