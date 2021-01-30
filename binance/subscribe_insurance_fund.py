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

measurement = "binance_insurance_fund"


future_tickers = bapi.get_exchange_info_future_stats()
symbols = [i['symbol'] for i in future_tickers["symbols"]]


def write_insurance_fund_data(symb,measurement,data):
    for d in data:
        fields = {}
        fields.update({"insurance_fund_balance_usdt":float(d["marginBalance"])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symb})
        db_time = datetime.datetime.utcfromtimestamp(d['insertTime']/1000)
        db.write_points_to_measurement(measurement,db_time,tags,fields)


def get_insurance_fund_hist_ticker(symb,measurement):
    insurance_fund = bapi.post_insurance_fund(symb,1)
    data = insurance_fund['data']
    write_insurance_fund_data(symb,measurement,data)
    total_pg = insurance_fund['total']
    pg = 1 
    while pg < total_pg:
        time.sleep(0.1)
        pg += 1
        insurance_fund = bapi.post_insurance_fund(symb,pg)
        data = insurance_fund['data']
        write_insurance_fund_data(symb,measurement,data)


def insurance_fund_update(measurement):
    symbols = [i['symbol'] for i in future_tickers["symbols"]]
    for symb in symbols:
        time.sleep(0.1)
        insurance_fund = bapi.post_insurance_fund(symb,1)
        data = insurance_fund['data']
        write_insurance_fund_data(symb,measurement,data)

def write_hist_data():
    for symb in symbols:
        print("writing" + symb)
        get_insurance_fund_hist_ticker(symb,measurement)
    
    
if __name__ == "__main__":
    insurance_fund_update(measurement)
    while True:
        # set up scheduler for 1 day
        time.sleep(60*60*24)
        try:
            insurance_fund_update(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)

