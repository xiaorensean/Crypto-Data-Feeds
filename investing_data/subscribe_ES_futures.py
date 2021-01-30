import traceback
from datetime import date, timedelta
import datetime
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(current_dir)
from investing_api.investingHtmlAPI import get_EC_futures

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


db = InfluxClient()
measurement = "sp500_futures"


def write_data(data,mesurement,realtime=None):
    if realtime is None:
        for d in data:
            fields = {i:d.get(i) for i in d if i != "Date"}
            tags = {}
            dbtime = d['Date']
            db.write_points_to_measurement(measurement,dbtime,tags,fields)
    else:
        fields = {i:data.get(i) for i in data if i != "Date"}
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        

def write_all_history(mesurement):
    start_ts = "02/15/1990"
    end_ts = "/".join(reversed(str(datetime.datetime.utcnow().date()).split("-")))
    get_all_data = False
    while not get_all_data:
        data = get_EC_futures(start_ts,end_ts)
        write_data(data,measurement)
        start_ts = "/".join(str(data[0]['Date'].date()).split("-")[1:] + [str(data[1]['Date'].date()).split("-")[0]])
        if str(data[0]['Date'].date()) == str(datetime.datetime.utcnow().date()):
            print("get all data")
            get_all_data = True
        else:
            pass
    
def write_live_data(measurement):
    end_ts = "/".join(str(datetime.datetime.utcnow().date()).split("-")[1:]+[str(datetime.datetime.utcnow().date()).split("-")[0]])
    start_ts = "/".join(str(date.today()-timedelta(days=1)).split("-")[1:]+[str(date.today()-timedelta(days=1)).split("-")[0]])
    data = get_EC_futures(start_ts,end_ts)
    if len(data) == 0:
        return
    else:
        realtime_data = data[0]   
        write_data(realtime_data,measurement,"Realtime")


def subscribe_ES_futures(measurement):
    try:
        data = db.query_tables(measurement,["*","order by time desc limit 1"])
    except IndexError:
        write_all_history(measurement)
    try: 
        write_live_data(measurement)
    except Exception:
        error = traceback.format_exc()
        logger(measurement,error)
        
    while True:
        time.sleep(1)
        try: 
            write_live_data(measurement)
        except Exception:
            error = traceback.format_exc()
            logger(measurement,error)
        
        
if __name__ == "__main__":
    subscribe_ES_futures(measurement)
