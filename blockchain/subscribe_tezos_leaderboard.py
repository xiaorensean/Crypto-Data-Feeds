import traceback
import time
import copy
import datetime
from bs4 import BeautifulSoup as bs
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

measurement = "tezos_leaderboard"

def subscribe_tezos_leaderboard(measurement):
    url = "https://tezos.fish/leaderboard"
    response = requests.get(url)
    page = response.content
    soup = bs(page)
    #printt = soup.prettify()

    # validator name
    validator_name_paritial = []
    for vn in soup.find_all("div",attrs={"class":"styled__Ellipsis-sc-1kfloow-8 eyDfyC"}):
        validator_name_paritial.append(vn.text)
    # address
    address = []
    for a in soup.find_all("span",attrs={"class":"atoms__Monospace-ubazf0-15 jnbnnM"}):
        address.append(a.text)
    # metadata
    meta_data = []
    for sa in soup.find_all("span",attrs={"class":"atoms__Number-ubazf0-13 jzqvEf"}):
        meta_data.append(sa.text)

    # rank, stake amount, percentage, delegators, fees 
    rank = [int(meta_data[idx]) for idx in range(0,400,4)]
    stake_amount = [float(st.replace(",","")) for st in [meta_data[idx][:-5] for idx in range(1,400,4)]]
    stake_amount_percentage = [float(meta_data[idx][-5:-1])/100 for idx in range(1,400,4)]
    delegators = [meta_data[idx] for idx in range(2,400,4)] 
    fees = [meta_data[idx] for idx in range(3,400,4)]

    # meta data
    meta_data_tr = []
    for md in soup.find_all("tr"):
        meta_data_tr.append(md.text)
        data_content = meta_data_tr[1:]

    validator_name_raw = []
    for idx,dc in enumerate(data_content):
        if len(dc.split("#")[1]) <= 85:
            validator_name_raw.append(None)
        else:
            validator_name_raw.append(dc.split("#")[1])

    validator_name = copy.copy(validator_name_raw)                                   
    index = []
    for idx in range(len(validator_name_raw)):
        if validator_name_raw[idx] is not None:
            index.append(idx)
        else:
            pass
    for ii in range(len(index)):
        try: 
            validator_name[index[ii]] = validator_name_paritial[ii] 
        except: 
            validator_name[index[ii]] = validator_name_raw[ii]
    # write to the database
    fields = {}
    current_snapshot = int(time.time()*1000)
    for idx in range(0,100):
        fields.update({"ref_ts":current_snapshot})
        fields.update({"rank":rank[idx]})
        fields.update({"validator_name":validator_name[idx]})
        fields.update({"validator_address":address[idx]})
        fields.update({"staked_amount":stake_amount[idx]})
        fields.update({"staked_amount_percentage":stake_amount_percentage[idx]})
        fields.update({"delegators":delegators[idx]})
        fields.update({"fees":fees[idx]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        

if __name__ == "__main__":
    try:
        subscribe_tezos_leaderboard(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        # set up scheduler for 1 hour
        time.sleep(60*60)
        try:
            subscribe_tezos_leaderboard(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
