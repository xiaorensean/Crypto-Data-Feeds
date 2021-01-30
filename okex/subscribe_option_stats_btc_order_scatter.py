import traceback
import datetime
import multiprocessing as mp
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
api_dir = os.path.dirname(current_dir) + "/okex/"
sys.path.append(api_dir)
import okex_api.option_stats_api as okexapi

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer


db = Writer()
measurement = "okex_btc_order_scatter"
def subscribe_btc_order_scatter():
    measurement = "okex_btc_order_scatter"
    data = okexapi.btc_order_scatter(symbol="BTC-USD")
    for idx in range(len(data['timestamps'])):
        fields = {}
        fields.update({"hist_vol":float(data["his_vol"][idx])})
        fields.update({"mark_vol": float(data["mark_vol"][idx])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":"BTCUSD"})
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamps'][idx]/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

data = okexapi.btc_order_scatter(symbol="BTC-USD")
for d in data:
    print(d)
print(data['call_volume'])
print(data['call_volume'][0])
print(data['call_volume'][1])
print(data["put_volume"])

'''''
if __name__ == "__main__":
    try:
        subscribe_btc_order_scatter()
    except:
        error_message = traceback.format_exc()
        logger(measurement, error_message)
    while True:
        time.sleep(60*58)
        try:
            subscribe_btc_order_scatter()
        except:
            error_message = traceback.format_exc()
            logger(measurement, error_message)
'''