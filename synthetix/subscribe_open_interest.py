import traceback
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from synx_api.SynxRestApi import get_open_interest
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

measurement = "synthetix_open_interest"


def subscribe_synx_open_interest(measurement):
    open_interest_data = get_open_interest()['openInterest']
    for oid in open_interest_data:
        fields = {}
        fields.update({"long_open_interest":float(oid["longs"])})
        fields.update({"short_open_interest":float(oid["total"])-float(oid['longs'])})
        fields.update({"total_open_interest":float(oid["total"])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":oid["name"]})
        dbtime = False
        db.write_points_to_measurement(measurement, dbtime, tags, fields)
    

if __name__ == "__main__":
    try:
        subscribe_synx_open_interest(measurement)
    except:
        pass
    while True:
        time.sleep(60*59)
        try:
            subscribe_synx_open_interest(measurement)  
        except Exception:
            error = traceback.format_exc()
            logger(measurement, error)
