import traceback
import time
import requests
from bs4 import BeautifulSoup as bs
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utils 
db = Writer()

def subscribe_burn_fees():
    webpage = "https://coinflex.com/flexcoin/"
    response = requests.get(webpage)
    soup = bs(response.text,'lxml')
    burn_fees = []
    for bfr in soup.findAll('div',attrs={'class':'h4 no-ling-height'}):
        temp = str(bfr)
        temp = temp.split(">")[1]
        temp = temp.split("<")[0]
        temp = temp.split(",")
        bf = "".join(temp)
        burn_fees.append(float(bf))
    # burn fees with name    
    burn_fees_dict = {"Yesterday's Total Revenue":0,
                  "Revenue above burn threshold":1,
                  "USDT to spend buying FLEX in current session":2,
                  "FLEX Acquisition Last 24 Hours":3}
    for cat in burn_fees_dict:
        burn_fees_dict.update({cat:burn_fees[burn_fees_dict[cat]]})

    measurement = "coinflex_burn_fees"
    tags = {}
    fields = burn_fees_dict.copy()
    fields.update({"is_api_return_timestamp": False})
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)

if __name__ == '__main__':
    subscribe_burn_fees()
    while True:
        time.sleep(60*60*24)
        try:
            subscribe_burn_fees()
        except Exception:
            error_message = traceback.format_exc()
            #logger("coinflex_burn_fees",error_message)
            