# Error: Expecting value: line 1 column 1 (char 0) 
# Reasons: 
# non-JSON conforming quoting
# XML/HTML output (that is, a string starting with <), or
# incompatible character encoding


import requests
import time
import datetime
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utils 
db = Writer()

def subscribe_borrow_rates():
    borrow_url = "https://api.dydx.exchange/v1/markets"
    response = requests.get(borrow_url)
    try:
        resp = response.json()
    except:
        # incurring json error for whatever reason
        # try for multiple time until sucess
        resp = None
        count = 0
        while count < 5:
            time.sleep(2)  
            borrow_url = "https://api.dydx.exchange/v1/markets"
            response = requests.get(borrow_url)
            try:
                resp = response.json()
            except:
                resp = None
            count += 1       
            if resp is not None:
                resp = resp
                break
            else:
                pass
        if resp is None:
            err = "Server side error: Json Error"
            logger("dydx_borrow_rates",err)
        else:
            pass
            
    if resp is not None:
        data = resp['markets']
        measurement = "dydx_borrow_rates"
        for d in data:
            fields = {}
            for dd in d:
                if dd == 'currency':
                    pass
                elif dd == 'symbol':
                    pass
                else:
                    try:
                        fields.update({dd:float(d[dd])})
                    except:
                        fields.update({dd: d[dd]})
            fields_clean = {f:fields[f] for f in fields if f != 'symbol' and f != 'updatedAt'}
            fields_clean.update({"is_api_return_timestamp": True})
            dbtime = d['updatedAt']
            tags = {}
            tags.update({"symbol":d["symbol"]})
            db.write_points_to_measurement(measurement,dbtime,tags,fields_clean)
    else:
        pass

if __name__ == '__main__':
    # collecting
    subscribe_borrow_rates()
    while (True):
        time.sleep(60)
        subscribe_borrow_rates()




