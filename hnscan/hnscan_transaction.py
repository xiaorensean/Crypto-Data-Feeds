import time
import math
import requests
import traceback
import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient

db = InfluxClient()


## api 
def get_block(offset,limit):
    url = "https://api.hnscan.com/blocks/?offset={}&limit={}".format(offset,limit)
    response = requests.get(url)
    resp = response.json()
    return resp

       
def hnscan_tx_update(all_data,measurement):
    txs = []
    for ad in all_data:
        txs += ad['tx']
    
    fields = {}
    for tx in txs:
        fields = {t:tx[t] for t in tx if t != "inputs" and t != "outputs"}
        fields.update({"inputs":"".join(tx["inputs"][0])})
        fields.update({"outputs":"".join(tx["outputs"][0])})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def get_full_hist_data():
    # get the latest block
    init = get_block(0,1)
    latest_height = init['total']
    counter = math.ceil(latest_height/50)
    all_data = []
    for idx in range(counter):
        offset = idx*50
        response = get_block(offset,50)
        diff_data = response['result']
        all_data += diff_data
    measurement = "hnscan_transaction"        
    hnscan_tx_update(all_data,measurement)

def subscribe_transaction():
    measurement = "hnscan_transaction" 
    init = get_block(0,1)
    latest_height = init['total'] - 1
    try:
        data_db = db.query_tables('hnscan_transaction',['max(height)',''])
        db_height = data_db['max'][0]
    except IndexError:
        get_full_hist_data()
        return
    height_diff = latest_height - db_height
    if height_diff < 50:
        response = get_block(0,height_diff)
        all_data = response['result']
    elif height_diff >= 50:
        counter = math.ceil(height_diff/50)
        all_data = []
        for idx in range(counter):
            offset = idx*50
            response = get_block(offset,50)
            diff_data = response['result']
            if idx == counter - 1:
                all_data += diff_data[:latest_height-offset]
            else:
                all_data += diff_data
    else:
        pass
    hnscan_tx_update(all_data,measurement)


if __name__ == "__main__":
    try:
        subscribe_transaction()
    except Exception:
        error_message = traceback.format_exc()
        if "referenced" in error_message:
            pass
        else:
            logger("hnscan_transaction",error_message)
    while True:
        time.sleep(60*60)
        try:
            subscribe_transaction()
        except Exception:
            error_message = traceback.format_exc()
            if "referenced" in error_message:
                pass
            else:
                logger("hnscan_transaction",error_message)
