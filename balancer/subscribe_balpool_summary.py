import traceback
from bs4 import BeautifulSoup as BeautifulSoup
import os
import sys
import datetime
import time
import requests
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(current_dir))

from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()
measurement = "balancer_pool_summary"

def scrpae_table():
    url = "http://www.predictions.exchange/balancer/None"
    response = requests.get(url)
    page = response.content
    soup = BeautifulSoup(page,"html.parser")

    table = soup.find('table',attrs={"class":"table table-hover"})
    table_body = table.find('tbody')
    # get raw data
    data_raw = []
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        fields = {}
        data_list = [ele for ele in cols if ele]  # Get rid of empty values
    fields = {}
    fields.update({"price":float(data_list[0].replace("$",""))})
    fields.update({"marketcap":float(data_list[1].replace("$","").replace(",",""))})
    fields.update({"total_liquidity":float(data_list[2].replace("$","").replace(",",""))})
    fields.update({"total_liquidity_adj": float(data_list[3].replace("$", "").replace(",", ""))})
    fields.update({"annual_bal":float(data_list[4].replace("$","").replace(",",""))})
    fields.update({"annual_bal_usd": float(data_list[5].replace("$", "").replace(",", ""))})
    fields.update({"bal_distributed": float(data_list[2].replace("$", "").replace(",", ""))})
    print(fields)
    return fields

def write_data(fields,measurement):
    for key, value in fields.items():
        if type(value) == int:
            fields[key] = float(value)
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_balacner_summary(measurement):
    data = scrpae_table()
    write_data(data,measurement)


if __name__ == "__main__":
    try:
        subscribe_balacner_summary(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        # set up scheduler for 1 minute
        time.sleep(57)
        try:
            subscribe_balacner_summary(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)

