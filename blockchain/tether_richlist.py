import traceback
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import numpy as np
import time
import datetime as dtt
import omni_api as api
import copy

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

# utils
add_tags = {'1FoWyxwPXuj4C6abqwhjDWdz6D4PZgYRjA':'Binance',
            '1H9HSzuzsV3R6fVHp9xPhVF9Z2mn3Lu3rP':'Binance',
            '1DUb2YYbQA1jjaNYzVXLZ7ZioEhLXtbUru':'Bittrex',
            '1KYiKJEfdJtap9QX2v9BXJMpz2SfU4pgZw':'Bitfinex',
            '1GjgKbj69hDB7YPQF9KwPEy274jLzBKVLh':'Bitfinex',
            '1DcKsGnjpD38bfj6RMxz945YwohZUTVLby':'Gate.io',
            '1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ':'Huobi',
            '16tg2RJuEPtZooy18Wxn2me2RhUdC94N7r':'Quarantined',
            '37Tm3Qz8Zw2VJrheUUhArDAoq58S6YrS3g':'OKEX',
            '1ApkXfxWgJ5CBHzrogVSKz23umMZ32wvNA':'OKEX',
            '1NTMakcgVwQpMdGxRQnFKyb3G1FAJysSfz':'Tether Treasury',
            '3GyeFJmQynJWd8DeACm4cdEnZcckAtrfcN':'Kraken',
            '1Po1oWkD2LmodfkBYiAktwh76vkF93LKnh':'Poloniex',
            '1Co1dhYDeF76DQyEyj4B5JdXF9J7TtfWWE':'Poloniex',
            }
current_ts = str(dtt.datetime.now())
db = InfluxClient()

def address_cluster(addresses,add_cls_len):
    address_clustered = [addresses[int(idx*add_cls_len):int(idx*add_cls_len+add_cls_len)] for idx in range(int(np.ceil(len(addresses)/add_cls_len)))]
    return address_clustered

def save_add_to_file():
    # read the richlist from saved excel
    df = pd.read_csv('richlist.csv')
    address_tether = df['Address'].tolist()
    
    # webscraping blockspur
    url = 'https://blockspur.com/tether/richlist'
    response = requests.get(url)
    if response.status_code == 200:
        page = response.content
        soup = bs(page,'lxml')
    else:
        print("Unable to request webpage")

    address_blockspur_temp = []
    for link in soup.findAll("a"):
        if len(link.get('href')) == 52:
            address_blockspur_temp.append(link.get('href'))
    address_blockspur = [i.split("/")[-1] for i in address_blockspur_temp]
    all_unique_address = pd.DataFrame(list(set(address_blockspur + address_tether)),columns=["address"])
    all_unique_address.to_csv("address.csv",index=False)

df = pd.read_csv(current_dir+'/configs/address.csv')
address = df['address'].tolist()
address_cluster = address_cluster(address,10)

# get account balance info for addresses in Tether
def account_balance_fetch(address_source):
    account_balance = []
    data_req = []
    for add in address_source:
        data_req.append(('add',add))
    response = api.v2_multi_address(data_req)
    for add in address_source:
        if add in add_tags.keys():
            remark = add_tags[add]
        else:
            remark = None
        # fetch balance as key value
        data = response[add]
        balance = data['balance']
        for b in balance:
            amt = int(b['value'])/100000000
            name = b['propertyinfo']['name']
            if name == 'TetherUS':
                if amt == 0:
                    frozen_amt = int(b['frozen'])/100000000
                    account_balance.append({"Address":add,"Amount":frozen_amt,"Remark":remark,"Update Time":current_ts})
                else:
                    account_balance.append({"Address":add,"Amount":amt,"Remark":remark, "Update Time":current_ts})
            else:
                pass
    return account_balance

def subscribe_to_richlist():
    # get the account balance for tether
    account_balance_agg = []
    for add_cls in address_cluster:
        time.sleep(30)
        account_balance = account_balance_fetch(add_cls)
        account_balance_agg += account_balance

    # sort amount descedingly 
    account_balance_agg_sorted = sorted(account_balance_agg, key=lambda k:k['Amount'],reverse=True)
    measurment = 'tether_richlist'
    for abl in account_balance_agg_sorted:
        fields = copy.copy(abl)
        fields.update({"is_api_return_timestamp": False})
        times = False
        tags = {}
        db.write_points_to_measurement(measurment,times,tags,fields)


subscribe_to_richlist()    
while True:
    time.sleep(60*60)
    try:
        subscribe_to_richlist()
    except Exception:
        error_message = traceback.format_exc()
        #logger('tether_richlist',error_message)
        
    


