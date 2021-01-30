import traceback
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bybit_api.bybitRestApi import funding_rates
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utils 
db = Writer()

# funding rates api is not listed in the api documet
# the api check for pages so if there is no next page 
# meaning that we get all the trades

def data_fetch(limit):
    resp = funding_rates(limit)
    if resp['next_page_url'] is None:
        data = resp['data']
    else:
        # check when more data to fetch
        print("Not get all data.")
        get_all_data = True
        limit = 10000
        increment_limit = 10
        while get_all_data:
            limit += increment_limit
            resp = funding_rates(limit)
            if resp['next_page_url'] is None:
                get_all_data = False
                data = resp['data']
            else:
                pass
    return data


# define updating logic
# input for all historical funding rate data
# input for updating the new updating funding rate data
def writing_data(data):
    measurement = "bybit_funding_rate"
    fields = {}
    for d in data:
        fields.update({"id":d["id"]})
        #fields.update({"symbol":d["symbol"]})
        fields.update({"funding_rate":float(d["value"])})
        fields.update({"is_api_return_timestamp": True})
        dbtime = d["time"]
        tags = {}
        tags.update({"symbol":d["symbol"]})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        


# subscribing funding rates data
def subscribe_funding_rate(limit):
    response = funding_rates(limit)
    updated_data = response['data']
    writing_data(updated_data)


if __name__ == "__main__":
    # historical data
    #historical_data = data_fetch(10000)
    #writing_data(historical_data)
    try:
        subscribe_funding_rate(8)
    except Exception:
        error_msg = traceback.format_exc()
        #logger("bybit_funding_rate",error_msg)
    while True:
        time.sleep(60*60*8)
        try:
            subscribe_funding_rate(8)
        except Exception:
            error_msg = traceback.format_exc()
            #logger("bybit_funding_rate",error_msg)
  
    
    


