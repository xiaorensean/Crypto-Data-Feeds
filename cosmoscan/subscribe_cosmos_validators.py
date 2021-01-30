import traceback
import os
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from cosmoscan_api.cosmosWsApi import get_validators

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "cosmoscan_validators"

def subscribe_validators(measurement):
    data = get_validators()['validators']
    for d in data:
        fields = {k:d[k] for k in d if k != "moniker" and k != "__typename" and k != "details"}
        fields.update({"tokens":int(d["tokens"])})
        fields.update({"commissionRate":float(d["commissionRate"])})
        #fields.update({"bondedHeight":int(d["bondedHeight"])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"moniker":d["moniker"]})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


if __name__ == "__main__":
    try:
        subscribe_validators(measurement)
    except:
        error = traceback.format_exc()
        #logger(measurement,error)
    while True:
        time.sleep(55)
        try:
            subscribe_validators(measurement)
        except:
            error = traceback.format_exc()
            #logger(measurement, error)

