import traceback
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from synx_api.SynxRestApi import synth_dahsboard_chart
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

measurement = "synthetix_dashboard_data"

def subscribe_synx_dashboard(measurement):
    dashboard_data = synth_dahsboard_chart()
    fields = dashboard_data
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement, dbtime, tags, fields)


if __name__ == "__main__":
    try:
        subscribe_synx_dashboard(measurement)
    except:
        pass
    while True:
        time.sleep(60*59)
        try:
            subscribe_synx_dashboard(measurement)  
        except Exception:
            error = traceback.format_exc()
            logger(measurement, error)
