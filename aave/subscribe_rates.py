import traceback
import os 
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from Aave_api.AaveWsApi import get_reserve_update, get_reserve_rate_hist_update

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "aave_rates"

def write_rates_data(data,measurement,symb=None):
    fields = {}
    adjusted = 10**27
    fields.update({"stableBorrowRate":float(data['stableBorrowRate'])/adjusted})
    fields.update({"variableBorrowRate":float(data['variableBorrowRate'])/adjusted})
    fields.update({"liquidationRate":float(data['liquidityRate'])/adjusted})
    fields.update({"utilizationRate":float(data['utilizationRate'])})
    fields.update({"is_api_return_timestamp":True})
    tags = {}
    try:
        tags.update({'symbol':data['symbol']})
    except:
        tags.update({'symbol':symb})
    try:
        dbtime = datetime.datetime.utcfromtimestamp(data['lastUpdateTimestamp'])
    except:
        dbtime = datetime.datetime.utcfromtimestamp(data['timestamp'])
    db.write_points_to_measurement(measurement, dbtime, tags, fields)

def subscribe_rate(measurement):
    data = get_reserve_update()
    for d in data:
        id = d['id']
        symbol = d['symbol']
        write_rates_data(d,measurement)
        hist_data = get_reserve_rate_hist_update(id)
        for hd in hist_data:
             write_rates_data(hd, measurement,symbol)

        
if __name__ == "__main__":
    subscribe_rate(measurement)
    while True:
        time.sleep(60*60)
        try:
            subscribe_rate(measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error)