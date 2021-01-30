import traceback
import datetime 
import time
import requests
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "okex_ticker_swap"

# function to automatical fet all tickers
def get_swap_tickers(base_token):
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "perpetual/pc/public/contracts/tickers?type={}".format(base_token)
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['data']
    return data


def swap_usd_update(swap_data,measurement):
    current_snapshot = str(datetime.datetime.utcnow())
    for d in swap_data:
        # float data type
        print(d)
        fields = {}
        # string data type
        fields = {}
        usd_oi = float(d['holdAmount']) * float(d['unitAmount'])
        coin_oi = float(d['holdAmount'])/(float(d['volume'])/float(d['coinVolume']))
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        fields.update({"changePercent":float(d["changePercent"])})
        fields.update({"close":float(d["close"])})
        fields.update({"coinVolume":float(d["coinVolume"])})
        fields.update({"coinName":d["coinName"]})
        fields.update({"high":float(d["high"])})
        fields.update({"holdAmount":float(d["holdAmount"])})
        fields.update({"low":float(d["low"])})
        fields.update({"open":float(d["open"])})
        fields.update({"unitAmount":float(d["unitAmount"])})
        fields.update({"volume":float(d["volume"])})
        fields.update({"type":d["type"]})
        fields.update({"contractId":int(d["contractId"])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":d["contract"]})
        dbtime = current_snapshot
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def swap_usdt_update(swap_data,measurement):
    current_snapshot = str(datetime.datetime.utcnow())
    for d in swap_data:
        # float data type
        print(d)
        fields = {}
        # string data type
        fields = {}
        coin_oi = float(d['holdAmount']) * float(d['unitAmount'])
        usd_oi = coin_oi * float(d['close'])
        fields.update({"coin_denominated_open_interest":float(coin_oi)})
        fields.update({"usd_denominated_open_interest":float(usd_oi)})
        fields.update({"changePercent":float(d["changePercent"])})
        fields.update({"close":float(d["close"])})
        fields.update({"coinVolume":float(d["coinVolume"])})
        fields.update({"coinName":d["coinName"]})
        fields.update({"high":float(d["high"])})
        fields.update({"holdAmount":float(d["holdAmount"])})
        fields.update({"low":float(d["low"])})
        fields.update({"open":float(d["open"])})
        fields.update({"unitAmount":float(d["unitAmount"])})
        fields.update({"volume":float(d["volume"])})
        fields.update({"type":d["type"]})
        fields.update({"contractId":int(d["contractId"])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":d["contract"]})
        dbtime = current_snapshot
        db.write_points_to_measurement(measurement,dbtime,tags,fields)



def subscribe_swap_ticker(measurement):
    usd_swap = get_swap_tickers("USD")
    usdt_swap = get_swap_tickers("USDT")  
    swap_usd_update(usd_swap,measurement)
    swap_usdt_update(usdt_swap,measurement)


if __name__ == '__main__':
    try:
        subscribe_swap_ticker(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(1)
        try:
            subscribe_swap_ticker(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)

