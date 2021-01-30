import traceback
import time
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
from hashpool_api.hashpoolRestApi import get_pool_coins


sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utility
db = Writer()

measurement = "hashpool_coins"

def subscribe_hashpool(measurement):
    data = get_pool_coins()
    if data['code'] == 0:
        hashpool = data['data']
    else:
        error_message = "Api fails"
        logger(measurement,error_message)
        hashpool = []
    # write data to influxdb
    for d in hashpool:
        fields = {}
        fields.update({"coin":d["coin"]})
        fields.update({"fee":float(d["fee"])})
        fields.update({"payoutThreshould":float(d["minPay"])})
        fields.update({"netHashRate":float(d["netHashrate"])})
        fields.update({"netHashRateUnit":d["netHashrateUnit"]})
        fields.update({"poolHashRate":float(d["poolHashrate"])})
        fields.update({"poolHashRateUnit":d["poolHashrateUnit"]})
        fields.update({"sort":int(d['sort'])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement, dbtime, tags, fields)

if __name__ == "__main__":
    # subscribe hashpool data
    try:
        subscribe_hashpool(measurement)
    except Exception:
        error = traceback.format_exc()
        logger(measurement,error)
    while True:
        time.sleep(60*60)
        try:
            subscribe_hashpool(measurement)
        except Exception:
            error = traceback.format_exc()
            logger(measurement,error)