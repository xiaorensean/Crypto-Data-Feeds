import traceback
import random
import time
import datetime
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger
from utility.timestamp_index_converter import unique_time_index_tranform


# utility
db = Writer()

def get_future_symbol():
    futures_symbol = bapi.get_futures_tickers()
    symbols = [ii['symbol'] for ii in futures_symbol]
    return symbols

def subscribe_trade(symb,market_type):
    measurement = "binance_trade_futures"
    trade_data_raw = bapi.get_trades(symb,market_type)
    trade_data = unique_time_index_tranform(trade_data_raw)
    fields = {}
    for td in trade_data:
        fields.update({"id":int(td['id'])})
        fields.update({"isBuyerMaker":td["isBuyerMaker"]})
        fields.update({"price":float(td["price"])})
        fields.update({"qty":float(td["qty"])})
        fields.update({"quoteQty":float(td["quoteQty"])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symb})
        db_time = datetime.datetime.utcfromtimestamp(td['time']/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)
        
        
if __name__ == "__main__":
    symbols_future = get_future_symbol()
    for symb in symbols_future:
        try:
            subscribe_trade(symb,"f")
            time.sleep(1)
        except Exception:
            error_message = traceback.format_exc()
            #logger('binance_trade_futures',error_message,symb)
        time.sleep(0.11)
    while True:
        # set up scheduler for 10 min
        time.sleep(60*10)
        symbols_future = get_future_symbol()
        for symb in symbols_future:
            try:
                subscribe_trade(symb,"f")
                time.sleep(1)
            except Exception:
                error_message = traceback.format_exc()
                #logger('binance_trade_futures',error_message,symb)
            time.sleep(0.1)
