import traceback
import time
import datetime
import random
import os
import sys
import requests

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

measurement_trade = 'ftx_trades_option'

# get trades
def ftx_options_trade():
    trade_endpoint = "https://ftx.com/api/options/trades"
    response = requests.get(trade_endpoint)
    resp = response.json()
    data = resp['result']
    return data


def subscribe_trades_option(measurement):
    data = ftx_options_trade()
    fields = {}
    for d in data:
        print(d)
        # raw info 
        fields.update({'id':d['id']})
        fields.update({'price':d['price']})
        fields.update({'size':d['size']})
        # option info
        option_info = d['option']
        ts = " ".join(option_info['expiry'].split("+")[0].split("T"))
        try:
            dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        except:
            dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        uts = int(time.mktime(dt_temp.timetuple()) * 1000)
        fields.update({'option_exipry': uts})
        fields.update({'strike_price':option_info['strike']})
        fields.update({'option_type':option_info['type']})
        fields.update({'underlying':option_info['underlying']})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        dbtime = d['time']
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


if __name__ == '__main__':
    try:
        subscribe_trades_option(measurement_trade)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement_trade,error_message)
        time.sleep(30)
        subscribe_trades_option(measurement_trade)
    while True:
        # set up scheduler for 1 second
        time.sleep(1)
        try:
            subscribe_trades_option(measurement_trade)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement_trade,error_message)
            time.sleep(2)
            subscribe_trades_option(measurement_trade)
          