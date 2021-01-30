import time
import datetime
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

def tickers():
    api_resp = bapi.get_exchange_info_future_stats()
    future_tickers = [i['symbol'] for i in api_resp['symbols']]
    return future_tickers

def write_to_database(data,data_len,measurement,symbol):
    fields = {}
    for i in range(0,len(data_len)):
        fields.update({"open_interest":data["open_interest"][i]})
        fields.update({"notional_value_open_interest":data["notional_value_open_interest"][i]})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symbol})
        tags.update({"frequency":data["frequency"]})
        db_time = datetime.datetime.utcfromtimestamp(data['timestamp'][i]/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)


def metadata_open_interest(symbol,freq,measurement,first=None):
    future_oi = bapi.post_open_interest(symbol,freq)
    if future_oi['code'] == '000000':
        future_oi_data = future_oi['data']
    else:
        error_message = "api error"
        tag = symbol + ":" + freq
        logger(measurement,error_message,tag)

    ts = future_oi_data['xAxis']
    data_sum_oi = []
    data_sum_oi_value = []
    if future_oi_data['series'][0]['name'] == "sum_open_interest":
        data_sum_oi = future_oi_data['series'][0]['data']
        data_sum_oi_value = future_oi_data['series'][1]['data']
    else:
        data_sum_oi = future_oi_data['series'][1]['data']
        data_sum_oi_value = future_oi_data['series'][0]['data']
        
    data = {}
    data.update({"symbol":symbol})
    data.update({"frequency":freq})
    data.update({"open_interest":data_sum_oi})
    data.update({"notional_value_open_interest":data_sum_oi_value})
    data.update({"timestamp":ts})
    write_to_database(data,data_sum_oi,measurement,symbol)



def subscribe_open_interest(freq,measurement):
    future_tickers = tickers()
    for symb in future_tickers:
        metadata_open_interest(symb,freq,measurement)
    while True:
        time.sleep(60*freq)
        future_tickers = tickers()
        for symb in future_tickers:
             metadata_open_interest(symb,freq,measurement)



        
