import traceback
import time
import datetime
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from curvefi_api.api_client import get_pool, get_apy_pool
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utils
db = Writer()
pool_list = get_pool()
print(pool_list)
freq_list = ["1", "5", "10", "30", "1440"]

def write_data(pool,data):
    measurement = "curve_pool_rates"
    lend_rate = data["lendRates"]
    for lr in lend_rate:
        fields = {}
        fields.update({"apy":lr["apy"]})
        fields.update({"apr":lr["apr"]})
        fields.update({"is_api_return_timestamp":False})
        tags = {}
        tags.update({"tokenSymbol":lr["tokenSymbol"]})
        tags.update({"poolSymbol":pool})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_pool_data():
    for pl in pool_list:
        apy = get_apy_pool(pl)
        write_data(pl,apy)

def subscribe_curve_pool_rates():
    write_pool_data()
    while True:
        time.sleep(60*15-10)
        write_pool_data()

if __name__ == "__main__":
    subscribe_curve_pool_rates()