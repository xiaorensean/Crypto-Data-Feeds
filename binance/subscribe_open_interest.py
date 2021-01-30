import traceback
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utility
db = Writer()

# get all futures tickers
def get_future_symbol():
    futures_symbol = bapi.get_futures_tickers()
    symbols = [ii['symbol'] for ii in futures_symbol]
    return symbols

# subscribe open interest
def subscribe_open_interest(symbol):
    measurement = "binance_open_interest"
    oi_data = bapi.get_open_interest(symbol)
    fields = {}
    fields.update({"open_interest":float(oi_data["openInterest"])})
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    tags.update({"symbol":oi_data["symbol"]})
    db_time = False
    db.write_points_to_measurement(measurement,db_time,tags,fields)

if __name__ == "__main__":
    symbols = get_future_symbol()
    for symb in symbols:
        try:
            subscribe_open_interest(symb)
        except Exception:
            error_message =  traceback.format_exc()
            #logger('binance_open_interest',error_message,symb)
    while True:
        # set up scheduler for 1 min
        time.sleep(60)
        symbols = get_future_symbol()
        for symb in symbols:
            try:
                subscribe_open_interest(symb)
            except Exception:
                error_message =  traceback.format_exc()
                #logger('binance_open_interest',error_message,symb)
