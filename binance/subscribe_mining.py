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

coin = bapi.get_coinlist()

def subscribe_mining_info():
    measurement = "binance_mining_algo"
    alg_data = bapi.get_algolist()['data']
    for ad in alg_data:
        fields = {}
        fields = {i:ad[i] for i in ad if i != 'algoName'}
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"algoName":ad["algoName"]})
        db_time = False
        db.write_points_to_measurement(measurement, db_time, tags, fields)

    measurement = "binance_mining_coin"
    coin_data = bapi.get_coinlist()['data']
    for cd in coin_data:
        fields = {}
        fields = {i:cd[i] for i in cd if i != 'coinName'}
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"coinName":cd["coinName"]})
        db_time = False
        db.write_points_to_measurement(measurement, db_time, tags, fields)

if __name__ == "__main__":
    try:
        subscribe_mining_info()
    except Exception:
        error_message = traceback.format_exc()
        #logger("binance_mining_info", error_message)
    while True:
        # set up scheduler for 1 min
        time.sleep(60 * 60 * 24)
        try:
            subscribe_mining_info()
        except Exception:
            error_message = traceback.format_exc()
            #logger("binance_mining_info", error_message)

