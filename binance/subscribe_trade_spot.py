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
from utility.timestamp_index_converter import unique_time_index_tranform

# utility
db = Writer()

# get all tickers
#exchange_info = bapi.get_exchange_info()
#symbols = exchange_info['symbols']
symbols_spot = ["DGDBTC","DGDETH","BCHUSDT","BCHBTC","BTCUSDT","ETHUSDT",\
                "KNCBTC","KNCETH","RENBTC","RENUSDT","RENBNB","BNBUSDT",\
                "BNBETH","BNBBTC","SOLBNB","SOLBTC","SOLBUSD","KNCUSDT",\
                "LINKUSDT", "LINKBTC", "LENDBTC","LENDUSDT", "LRCBTC",\
                "LRCUSDT","REPBTC","REPUSDT","KAVABTC","KAVAUSDT","ASTBTC",\
                "BNTBTC","BNTUSDT","BANDBTC","BANDUSDT","INJBTC","INJBNB",\
                "INJBUSD", "INJUSDT"]

def subscribe_trade(symb,market_type):
    measurement = "binance_trade_spot"
    trade_data_raw = bapi.get_trades(symb,market_type)
    if len(trade_data_raw) != 2:
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
    else:
        print("Error message ---", trade_data_raw["msg"])
        return


if __name__ == "__main__":
    for symb in symbols_spot:
        try:
            subscribe_trade(symb,"s")
            time.sleep(1)
        except Exception:
            error_message = traceback.format_exc()
            #logger('binance_trade_spot',error_message,symb)
    while True:
        # set up scheduler for 10 minnutes
        time.sleep(60*10)
        for symb in symbols_spot:
            try:
                subscribe_trade(symb,"s")
                time.sleep(1)
            except Exception:
                error_message = traceback.format_exc()
                #logger('binance_trade_spot',error_message,symb)
