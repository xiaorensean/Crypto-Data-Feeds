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
measurement = "balancer_pool"

def scrpae_table():
    url = "http://www.predictions.exchange/balancer/None"
    response = requests.get(url)
    page = response.content
    soup = BeautifulSoup(page,"html.parser")

    # get the table within form
    form = soup.find('form')
    table = form.find('table',attrs={"class":"table table-hover"})
    table_body = table.find('tbody')
    # get raw data
    data_raw = []
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        fields = {}
        data_list = [ele for ele in cols if ele] # Get rid of empty values
        print(data_list)
        #asset_data = data_list[1].split(" ")
        #asset = asset_data[0::2]
        #asset_comp = asset_data[1::2]
        #for idx in range(len(asset)):
        #    fields.update({"asset_weight"+asset[idx]:float(asset_comp[idx].split("%")[0])/100})
        fields.update({"pool":data_list[0]})
        fields.update({"assets":data_list[1]})
        fields.update({"fee":float(data_list[2].split("%")[0])/100})
        fields.update({"liquidity":float(data_list[3].split("$")[1].replace(",",""))})
        fields.update({"total_factor":float(data_list[4])})
        fields.update({"adj_liquidity":float(data_list[5].split("$")[1].replace(",",""))})
        fields.update({"annual_bal":float(data_list[6].replace(",",""))})
        fields.update({"annual_bal_usd":float(data_list[7].split("$")[1].replace(",",""))})
        fields.update({"RoL":float(data_list[8].split("%")[0])/100})
        data_raw.append(fields)


    # get full address
    address_raw = []
    for link in form.find_all("a",href=True):
        address_raw.append(link['href'])

    address = list(set([adr.split("/")[-1] for adr in address_raw if "#" != adr]))
    data = []
    for dr in data_raw:
        init = dr['pool'][:4]
        last = dr['pool'][-4:]
        for add in address:
            if init in add and last in add:
                dr.update({"pool": add})
                data.append(dr)
            else:
                pass
    return data

def write_data(data,measurement):
    for d in data:
        fields = {k:d[k] for k in d if k != 'pool'}
        for key, value in fields.items():
            if type(value) == int:
                fields[key] = float(value)
        fields.update({"is_api_return_timestamp": False})
        fields.update({"ref_ts":int(time.time())})
        tags = {}
        tags.update({"address":d["pool"]})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_balacner_pool(measurement):
    data = scrpae_table()
    write_data(data,measurement)


if __name__ == "__main__":
    try:
        subscribe_balacner_pool(measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)
    while True:
        # set up scheduler for 15 minutes
        time.sleep(15*60-5)
        try:
            subscribe_balacner_pool(measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message)

