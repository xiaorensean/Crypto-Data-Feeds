import time
import traceback
import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))
from hnscan_api.hnscanApi import get_summary, get_status
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer

db = Writer()

status = get_status()
# hnscan summary    
def subscribe_hnscan_summary(measurement):
    summary = get_summary()
    fields = {s:summary[s] for s in summary if s != "hashrate" and s != "difficulty"}
    fields.update({"hashrate":float(summary["hashrate"])})
    fields.update({"difficulty":float(summary["difficulty"])})
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)
  
# hnscan status     
def subscribe_hnscan_status(measurement):
    status = get_status()
    fields = {s:status[s] for s in status if s != "difficulty" and s != "progress"}
    fields.update({"difficulty":float(status["difficulty"])})
    fields.update({"progress":int(status["progress"])})
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)




if __name__ == "__main__":
    measurement = "hnscan_summary"
    subscribe_hnscan_summary(measurement)
    measurement = "hnscan_status"
    subscribe_hnscan_status(measurement)    
    while True:
        time.sleep(60*9)
        try:
            measurement = "hnscan_summary"
            subscribe_hnscan_summary(measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message)
        try:
            measurement = "hnscan_status"
            subscribe_hnscan_status(measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message)
    
    
