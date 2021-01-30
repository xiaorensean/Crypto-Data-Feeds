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

future_tickers = bapi.get_exchange_info_future_stats()
symbols = [i['symbol'] for i in future_tickers["symbols"]]
measurement = "binance_funding_rates"
       

def subscribe_funding_rates(symb,measurement,limit):
    funding_rates = bapi.get_funding_rates(symb,limit)
    fields = {}
    for frd in funding_rates:
        fields.update({"fundingRate":float(frd["fundingRate"])})
        fields.update({"fundingTime":frd["fundingTime"]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol": symb})
        db_time = False
        db.write_points_to_measurement(measurement,db_time,tags,fields)
            

if __name__ == "__main__":
    for symb in symbols:
        try:
            subscribe_funding_rates(symb,measurement,1000)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,symb)
    while True:
        # set up scheduler for 1 min
        time.sleep(60*60*8)
        for symb in symbols:
            try:
                subscribe_funding_rates(symb,measurement,1000)
            except Exception:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symb)

