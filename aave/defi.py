import json
from bs4 import BeautifulSoup as bs
import requests
import traceback
import os 
import sys
import time
current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


db = InfluxClient()



def get_aave_rate(type):
    url = "https://defirate.com/lend/?exchange_table_type={}".format(type)
    response = requests.get(url)
    page = response.content
    soup = bs(page)
    # get all script tags data
    data = []
    for sa in soup.find_all("script"):
        data.append(sa.text)
    # get aave script data 
    aave = []
    for d in data:
        if "Aave" in d:
            # convert string dict to dict
            json_acceptable_string = d.replace("'", "\"")
            d = json.loads(json_acceptable_string)
            aave.append(d)
        else:
            pass

    if type == "borrow":
        aave_rate = [{aa['name'].split(" ")[1]:float(aa['annualPercentageRate'][0]['value'])/100} for aa in aave[:17]]
                 
    elif type == "lend":
        if "DAI" in aave[17]['name']:
            aave_rate = [{aa['name'].split(" ")[1]:float(aa['interestRate'])/100} for aa in aave[:17]]
        else:
            aave_rate = [{aa['name'].split(" ")[1]:float(aa['interestRate'])/100} for aa in aave[:16]]
            aave_rate.append({"LEND":float(0)})
    else:
        pass
    return aave_rate

type = "borrow"
rates = get_aave_rate(type)
