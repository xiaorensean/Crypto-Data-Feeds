import traceback
import time
import datetime
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


# subscribe liquidation_trades
def subscribe_liquidation_trades(symbol):
    measurement = "binance_liquidation_trades"
    liquidation_data =  bapi.get_liquidation_orders(symbol)
    for ld in liquidation_data:
        fields = {}
        fields.update({"averagePrice":float(ld['averagePrice'])})
        fields.update({"executedQty":float(ld['executedQty'])})
        fields.update({"origQty":float(ld['origQty'])})
        fields.update({"price":float(ld['price'])})
        fields.update({"side":ld['side']})
        fields.update({"status":ld['status']})
        fields.update({"type":ld['type']})
        fields.update({"timeInForce":ld['timeInForce']})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":ld["symbol"]})
        db_time = datetime.datetime.utcfromtimestamp(ld['time']/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)
                                  

if __name__ == "__main__":
    symbols = get_future_symbol()
    for symb in symbols:
        try:
            subscribe_liquidation_trades(symb)
        except Exception:
            error_message = traceback.format_exc()
            #logger('binance_liquidation_trades',error_message,symb)
    while True:
        # set up scheduler for 1 hour
        time.sleep(60*60)
        symbols = get_future_symbol()
        for symb in symbols:
            try:
                subscribe_liquidation_trades(symb)
            except Exception:
                error_message = traceback.format_exc()
                #logger('binance_liquidation_trades',error_message,symb)
