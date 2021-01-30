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

measurement = "okex_btc_options_openInterest_tradingVolume"
def write_btc_oi_vol(freq):
    measurement = "okex_btc_options_openInterest_tradingVolume"
    data = okexapi.btc_options_oi_tradingVol(freq,symbol="BTC-USD")
    for idx in range(len(data['call_position'])):
        fields = {}
        fields.update({"open_interest_call":float(data['call_position'][idx])})
        fields.update({"open_interest_put": float(data['put_position'][idx])})
        fields.update({"volume_call": float(data['call_volume'][idx])})
        fields.update({"volume_put": float(data['put_volume'][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        tags.update({"frequency":freq})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_btc_oi_vol_strike(freq,delivery_time):
    measurement = "okex_btc_options_openInterest_tradingVolume_strike"
    data = okexapi.btc_options_oi_tradingVol_strike(freq, delivery_time, symbol="BTC-USD")
    for idx in range(len(data['call_position'])):
        fields = {}
        fields.update({"open_interest_call":float(data['call_position'][idx])})
        fields.update({"open_interest_put": float(data['put_position'][idx])})
        fields.update({"volume_call": float(data['call_volume'][idx])})
        fields.update({"volume_put": float(data['put_volume'][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        tags.update({"frequency":freq})
        tags.update({"delivery_time":delivery_time})
        tags.update({"strike":data['strike'][idx]})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_btc_oi_vol_expiry(freq):
    measurement = "okex_btc_options_openInterest_tradingVolume_expiry"
    data = okexapi.btc_options_oi_tradingVol_expiry(freq,symbol="BTC-USD")
    for idx in range(len(data['call_position'])):
        fields = {}
        fields.update({"open_interest_call":float(data['call_position'][idx])})
        fields.update({"open_interest_put": float(data['put_position'][idx])})
        fields.update({"volume_call": float(data['call_volume'][idx])})
        fields.update({"volume_put": float(data['put_volume'][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        tags.update({"frequency":freq})
        tags.update({"delivery_time":data['delivery_time'][idx]})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_btc_call_put_ratio(freq):
    measurement = "okex_btc_options_call_put_ratio"
    data = okexapi.btc_options_call_put_ratios(freq)
    for idx in range(len(data['open_interest_ratio'])):
        fields = {}
        fields.update({"open_interest_ratio":float(data['open_interest_ratio'][idx])})
        fields.update({"trading_volume_ratio": float(data['trading_volume_ratio'][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        tags.update({"frequency":freq})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_btc_taker_flow(freq):
    measurement = "okex_btc_options_taker_flow"
    data = okexapi.btc_options_takerflow(freq,symbol="BTC-USD")
    fields = data
    for key, value in fields.items():
        if type(value) == int:
            fields[key] = float(value)
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    tags.update({"symbol":"BTCUSD"})
    tags.update({"frequency":freq})
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)


def write_metadata(freq):
    # subscribe oi vol
    write_btc_oi_vol(freq)
    # subscribe oi vol strike
    delivery_times = okexapi.btc_options_oi_tradingVol_expiry(freq,symbol="BTC-USD")['delivery_time']
    for dt in delivery_times:
        write_btc_oi_vol_strike(freq,dt)
    # subscribe oi vol expiry
    write_btc_oi_vol_expiry(freq)
    # subscribe call put ratio
    write_btc_call_put_ratio(freq)
    # subscribe options taker flow
    write_btc_taker_flow(freq)


def subscribe_data(freq):
    try:
        write_metadata(freq)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement, error_message)
    while True:
        time.sleep(freq-60)
        try:
            write_metadata(freq)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement, error_message)


if __name__ == '__main__':
    # all processes
    frequency = [8*60*60,24*60*60]
    processes = {}
    for freq in frequency:
        data = mp.Process(target=subscribe_data, args=(freq,))
        data.start()
        processes.update({freq: data})
