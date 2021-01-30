import time
import traceback
import datetime
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger
future_tickers = bapi.get_exchange_info_future_stats()

db = Writer()

def tickers():
    api_resp = bapi.get_exchange_info_future_stats()
    future_tickers = [i['symbol'] for i in api_resp['symbols']]
    return future_tickers

def write_to_database_long_short(data,data_len,measurement,symbol):
    fields = {}
    for i in range(0,len(data_len)):
        fields.update({"long_short_ratio":float(data["long_short_ratio"][i])})
        fields.update({"long_account":data["long_account"][i]})
        fields.update({"short_account":data["short_account"][i]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":data["frequency"]})
        db_time = datetime.datetime.utcfromtimestamp(data['timestamp'][i]/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)

def write_to_database_volume(data,data_len,measurement,symbol):
    fields = {}
    for i in range(0,len(data_len)):
        fields.update({"long_short_ratio":float(data["long_short_ratio"][i])})
        fields.update({"long_volume":data["long_volume"][i]})
        fields.update({"short_volume":data["short_volume"][i]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":data["frequency"]})
        db_time = datetime.datetime.utcfromtimestamp(data['timestamp'][i]/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)


def metadata_long_short_ratio(symbol,freq,measurement,lsr_type):
    if lsr_type == "global":
        future_lsr = bapi.post_long_short_ratio_level(symbol,freq)
        write_data_long_short_ratio(future_lsr,symbol,freq,measurement,lsr_type)
    elif lsr_type == "account":
        future_lsr = bapi.post_long_short_ratio_account(symbol,freq)
        write_data_long_short_ratio(future_lsr,symbol,freq,measurement,lsr_type)
    elif lsr_type == "position":
        future_lsr = bapi.post_long_short_ratio_position(symbol,freq)
        write_data_long_short_ratio(future_lsr,symbol,freq,measurement,lsr_type)
    elif lsr_type == "volume":
        future_lsr = bapi.post_taker_buy_sell_volume(symbol,freq)
        write_data_taker_buy_sell_volume(future_lsr,symbol,freq,measurement,lsr_type)
    else:
        print("error")

def write_data_long_short_ratio(future_lsr,symbol,freq,measurement,lsr_type):
    if future_lsr['code'] == '000000':
        future_lsr_data = future_lsr['data']
    else:
        error_message = "api call incurs error"
        tag = symbol + ":" + freq
        logger(measurement,error_message,tag)

    ts = future_lsr_data['xAxis']
    data_long_short_ratio = future_lsr_data['series'][0]['data']
    data_long_account = future_lsr_data['series'][1]['data']
    data_short_account = future_lsr_data['series'][2]['data']
    
    data = {}
    data.update({"symbol":symbol})
    data.update({"frequency":freq})
    data.update({"long_short_ratio":data_long_short_ratio})
    data.update({"long_account":data_long_account})
    data.update({"short_account":data_short_account})
    data.update({"timestamp":ts})
    write_to_database_long_short(data,data_long_short_ratio,measurement,symbol)


def write_data_taker_buy_sell_volume(future_lsr,symbol,freq,measurement,lsr_type):
    if future_lsr['code'] == '000000':
        future_lsr_data = future_lsr['data']
    else:
        error_message = "api call incurs error"
        tag = symbol + ":" + freq
        logger(measurement,error_message,tag)

    ts = future_lsr_data['xAxis']
    data_long_short_ratio = future_lsr_data['series'][0]['data']
    data_long_account = future_lsr_data['series'][1]['data']
    data_short_account = future_lsr_data['series'][2]['data']
    
    data = {}
    data.update({"symbol":symbol})
    data.update({"frequency":freq})
    data.update({"long_short_ratio":data_long_short_ratio})
    data.update({"long_volume":data_long_account})
    data.update({"short_volume":data_short_account})
    data.update({"timestamp":ts})
    write_to_database_volume(data,data_long_short_ratio,measurement,symbol)

def subscribe_long_short_ratio(freq,measurement,lsr_type):
    future_tickers = tickers()
    for symb in future_tickers:
        metadata_long_short_ratio(symb,freq,measurement,lsr_type)
    while True:
        time.sleep(60*freq)
        future_tickers = tickers()
        for symb in future_tickers:
             metadata_long_short_ratio(symb,freq,measurement,lsr_type)

