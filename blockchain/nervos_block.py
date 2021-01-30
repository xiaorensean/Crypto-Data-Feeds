import traceback
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__)) 
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger
import time
import requests
import pandas as pd



# utils
link = 'https://api.explorer.nervos.org/api/v1/statistics/blockchain_info'
db = InfluxClient()


def blockchain(url):
    headers = { 'Accept': 'application/vnd.api+json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Sa', 
          'Content-Type': 'application/vnd.api+json'} 
    data = requests.get(url, headers = headers).json()['data']
    d = data['attributes']['blockchain_info']
    dataframe = pd.DataFrame(list(d.values())).T
    cols = list(d.keys())
    dataframe.columns = cols
    dataframe.insert(0, 'id', data['id'])
    dataframe.insert(1, 'type', data['type'])
    alert_check = len(dataframe.alerts.values.tolist()[0]) 
    if alert_check == 0:
        dataframe.alerts = False
    else:
        dataframe.alerts = dataframe.alerts.values.tolist()[0]
    measurement = "nervos_block"
    df_2_dict = dataframe.T.to_dict()[0]
    fields = {}
    times = False
    tags = {}
    fields = df_2_dict
    fields.update({"is_api_return_timestamp": False})
    db.write_points_to_measurement(measurement,times,tags,fields) 
    return 


try:
    blockchain(link)
except:
    error = traceback.format_exc()
    #logger("nervos_block", error)
while True:
    time.sleep(60*10)
    try:
        blockchain(link)
    except:
        error = traceback.format_exc()
        #logger("nervos_block", error)
    

    




