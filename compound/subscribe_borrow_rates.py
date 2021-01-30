import traceback
import requests
import time
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
    borrow_url = "https://api.compound.finance/api/v2/ctoken?meta=true"
    response = requests.get(borrow_url)
    resp = response.json()
    data = resp['cToken']

    measurement = "compound_borrow_rates"
    for d in data:
        fields = {}
        for f in d:
            if isinstance(d[f],dict):
                value = d[f]['value']
            else:
                value = d[f]
            try:
                fields.update({f:float(value)})
            except:
                fields.update({f:value})
        fields_clean = {f:fields[f] for f in fields if f != 'symbol'}
        fields_clean.update({"is_api_return_timestamp": False})
        dbtime = False
        tags = {}
        tags.update({"symbol":fields["symbol"]})
        db.write_points_to_measurement(measurement,dbtime,tags,fields_clean)


if __name__ == '__main__':
    try:
        subscribe_borrow_rates()
    except Exception:
        error = traceback.format_exc()
        #logger("compound_borrow_rates",error)
    while (True):
        time.sleep(60)
        try:
            subscribe_borrow_rates()
        except Exception:
            pass
            #logger("compound_borrow_rates",error)

