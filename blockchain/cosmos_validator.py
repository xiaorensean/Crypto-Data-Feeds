import traceback
import time
import datetime
import requests
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utility
db = Writer()

# get data 
def get_data(endpoint):
   response = requests.get(endpoint)
   resp = response.json()
   return resp


# cosmos validator status 
def subscribe_status():
    status_endpoint = "https://api.cosmostation.io/v1/status"
    status_data = get_data(status_endpoint)
    print(status_data)
    for f in status_data:
        if type(f) == list:
            pass
        else:
            fields = {f:status_data[f]}
    fields.update({"is_api_return_timestamp": True})
    measurement = "cosmos_validator_status"
    tags = {}
    dbtime = status_data['timestamp']
    db.write_points_to_measurement(measurement,dbtime,tags,fields)
    
def subscribe_validator_rank():
    validator_endpoint = "https://api.cosmostation.io/v1/staking/validators"
    validator_ranking_data = get_data(validator_endpoint)
    measurement = "cosmos_validator_ranking"
    fields = {}
    current_snapshot = datetime.datetime.now()
    for data in validator_ranking_data:
        fields = {x:data[x] for x in data if x not in "uptime"}
        fields.update({"uptime_over_blocks":data["uptime"]["over_blocks"]})
        fields.update({"uptime_address":data["uptime"]["address"]})
        fields.update({"uptime_missed_blocks":data["uptime"]["missed_blocks"]})
        fields.update({"current_snapshot":str(current_snapshot)})
        fields.update({"is_api_return_timestamp": False})
        if data['website'] == "stake.fish":
            fields.update({"moniker":"stake.fish"})
        else:
            pass
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
    

# start collecting for funding rates data
if __name__ == "__main__":
    try:
        subscribe_status()
        subscribe_validator_rank()
    except Exception:
        error_message = traceback.format_exc()
        #logger('cosmos_validator_status',error_message)
        time.sleep(30)
        subscribe_status()
        subscribe_validator_rank()
    while True:
        # set up scheduler for 30 mins
        time.sleep(60*60*12)
        try:
            subscribe_status()
            subscribe_validator_rank()
        except Exception:
            error_message = traceback.format_exc()
            #logger('cosmos_validator_status',error_message)
            time.sleep(30)
            subscribe_status()
