import time
import datetime
import traceback
import copy
import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))
from hnscan_api.hnscanApi import *
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer

db = Writer()

# hnscan data 
def subscribe_hnscan_data(measurement):
    current_time = int(time.time()) 
    start_ts = current_time
    end_ts = 1494009073
    difficulty = [{'date':d['date'],'difficulty':float(d['value'])} for d in get_difficulty(start_ts,end_ts)]
    daily_tx = [{'date':d['date'],'daily_tx':d['value']} for d in get_daily_tx(start_ts,end_ts)]
    daily_totaltx = [{'date':d['date'],'total_tx':d['value']} for d in get_daily_totaltx(start_ts,end_ts)]
    supply = [{'date':d['date'],'supply':d['value']} for d in get_supply(start_ts,end_ts)]
    burned = [{'date':d['date'],'burned':d['value']} for d in get_burned(start_ts,end_ts)]
    all_data = copy.copy(difficulty)
    for idx,dict_obj in enumerate(all_data):
        dict_obj.update({'daily_tx':int(daily_tx[idx]['daily_tx'])})
        dict_obj.update({'total_tx':int(daily_totaltx[idx]['total_tx'])})
        dict_obj.update({'supply':float(supply[idx]['supply'])})
        dict_obj.update({'burned':float(burned[idx]['burned'])})
    # all data fields
    for ad in all_data:
        fields = {i:ad[i] for i in ad if i != 'date'}
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        dbtime = datetime.datetime.utcfromtimestamp(ad['date']/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

# hnscan data 
def subscribe_hnscan_data_recon(measurement):
    current_time = int(time.time())
    start_ts = current_time
    end_ts = 1494009073
    difficulty = [{'date':d['date'],'difficulty':float(d['value'])} for d in get_difficulty(start_ts,end_ts)]
    daily_tx = [{'date':d['date'],'daily_tx':d['value']} for d in get_daily_tx(start_ts,end_ts)]
    daily_totaltx = [{'date':d['date'],'total_tx':d['value']} for d in get_daily_totaltx(start_ts,end_ts)]
    supply = [{'date':d['date'],'supply':d['value']} for d in get_supply(start_ts,end_ts)]
    burned = [{'date':d['date'],'burned':d['value']} for d in get_burned(start_ts,end_ts)]
    all_data = copy.copy(difficulty)
    for idx,dict_obj in enumerate(all_data):
        dict_obj.update({'daily_tx':int(daily_tx[idx]['daily_tx'])})
        dict_obj.update({'total_tx':int(daily_totaltx[idx]['total_tx'])})
        dict_obj.update({'supply':float(supply[idx]['supply'])})
        dict_obj.update({'burned':float(burned[idx]['burned'])})
    # all data fields
    for ad in all_data:
        fields = ad
        fields.update({"current_snapshot":current_time})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

 
if __name__ == "__main__":
    measurement = "hnscan_chart_data"
    subscribe_hnscan_data(measurement)
    measurement = "hnscan_chart_data_snapshot"
    subscribe_hnscan_data_recon(measurement)
    while True:
        time.sleep(60*60*24)
        try:
            measurement = "hnscan_chart_data"
            subscribe_hnscan_data(measurement)
            measurement = "hnscan_chart_data_snapshot"
            subscribe_hnscan_data_recon(measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message)
   
    
    
