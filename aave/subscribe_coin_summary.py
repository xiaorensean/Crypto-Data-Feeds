import traceback
import os
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from Aave_api.AaveWsApi import get_priceOracle, get_summary

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()
measurement = "aave_burn_summary"
def subscribe_burn_summary(measurement):
    data = get_summary()
    try:
        data["errors"]
        return
    except:
        summary = data['data']['burn']
        fields = {}
        fields.update({"totalBurnedLEND":float(summary['totalBurnedLEND'])})
        fields.update({'totalReadtToBurnLEND':float(summary['totalReadyToBurnLEND'])})
        fields.update({"lendPrice":float(summary['lendPrice'])})
        fields.update({"marketCap":float(summary['marketCap'])})
        fields.update({"volume24Hours":float(summary['volume24Hours'])})
        fields.update({"is_api_return_timestamp":False})
        dbtime = False
        tags = {}
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

if __name__ == "__main__":
    subscribe_burn_summary(measurement)
    while True:
        time.sleep(60)
        subscribe_burn_summary(measurement)