import traceback
import os 
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from coinbase_api.coinbaseRestApi import get_custody

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()


measurement = "coinbase_custody"


# write bid ask data to influxdb 
def write_custody_data(measurement):
    custody_data = get_custody()
    fields = {}
    current_utctime = str(datetime.datetime.utcnow())
    for d in custody_data:
        fields.update({"name":d["name"]})
        if d["market_cap"] is None:
            fields.update({"market_cap":None})
        else:
            fields.update({"market_cap":float(d["market_cap"])})
        fields.update({"icon":d["icon"]})
        fields.update({"price":float(d["price"])})
        fields.update({"is_api_return_timestamp":False})
        tags = {}
        tags.update({"symbol":d["symbol"]})
        dbtime = current_utctime
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   


def subscribe_custody(measurement):
    try:
        write_custody_data(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(60*60)
        try:
            write_custody_data(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            time.sleep(1)



if __name__ == '__main__':    
    subscribe_custody(measurement)

