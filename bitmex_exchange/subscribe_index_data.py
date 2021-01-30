import time
import requests
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.notification import send_error_message
from influxdb_client.influxdb_writer import Writer


base_url = "https://www.bitmex.com/api/v1/"
measurement = 'bitmex_index_composite'
db = Writer()


def index_composite(symbol):
    index_composite_endpoint = base_url+'instrument/compositeIndex?symbol={}&reverse=true&count=7'
    index_composite_endpoint = index_composite_endpoint.format(symbol)
    response = requests.get(index_composite_endpoint)
    data = response.json()
    return data


def subscribe_index(measurment):
    btc_index = ".BXBT"
    index_composite_data = index_composite(btc_index)
    for ic in index_composite_data:
        fields = ic 
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   


if __name__ == '__main__':
    try:
        subscribe_index(measurement)
    except Exception as err:
        error_message = str(err)
        send_error_message(measurement,error_message)
        time.sleep(30)
        subscribe_index(measurement)
    while True:
        # set up scheduler for 1 second
        time.sleep(5)
        try:
            subscribe_index(measurement)
        except Exception as err:
            error_message = str(err)
            send_error_message(measurement,error_message)
            time.sleep(2)
            subscribe_index(measurement)
            