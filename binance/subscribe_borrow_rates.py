import traceback
import datetime as dt
import requests
import time
import os 
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utility
db = Writer()

# define api end_points
margin_end_points = "https://www.binance.com/gateway-api/v1/public/margin/vip/spec/list-all"

def subscribe_funding_rates():
    # request web page
    web_page = requests.get(margin_end_points)

    # get content 
    response = web_page.json()
    if response['success'] == True:
        data = response['data']
    
    # define the timestamp that the data is fetched 
    current_utc_time = str(dt.datetime.utcnow())   

    # define measrement 
    measurement = 'binance_borrow_rates'

    # get all data and write it to the database      
    for d in data:
        symb = d['assetName']
        spec = d['specs']
        fields = {}
        for de in spec:
            fields.update({"borrowLimit":float(de["borrowLimit"])})
            fields.update({"dailyInterestRate":float(de["dailyInterestRate"])})
            fields.update({"vipLevel":de["vipLevel"]})
            fields.update({"is_api_return_timestamp": False})
            tags = {}
            tags.update({'symbol':symb})
            dbtime = current_utc_time
            db.write_points_to_measurement(measurement,dbtime,tags,fields)

# start collecting for funding rates data
if __name__ == "__main__":
    try:
        subscribe_funding_rates()
    except Exception:
        error = traceback.format_exc()
        logger('binance_borrow_rates',error)
        time.sleep(30)
        subscribe_funding_rates()
    while True:
        # set up scheduler for 30 mins
        time.sleep(60*30)
        try:
            subscribe_funding_rates()
        except Exception:
            error = traceback.format_exc()
            logger('binance_borrow_rates',error)
            time.sleep(30)
            subscribe_funding_rates()
            
            
            
