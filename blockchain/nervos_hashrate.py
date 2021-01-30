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
link = 'https://api.explorer.nervos.org/api/v1/statistics/'
db = InfluxClient()


def blockchain(url):
    headers = { 'Accept': 'application/vnd.api+json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Sa', 
          'Content-Type': 'application/vnd.api+json'} 
    data = requests.get(url, headers = headers).json()['data']
    attr_raw = data['attributes']
    # api changes for epoch_info cell
    attr = {}
    for i in attr_raw:
        if i == "epoch_info":
            pass
        else:
            attr.update({i:attr_raw[i]}) 
    attr.update(attr_raw['epoch_info'])
    dataframe = pd.DataFrame(list(attr.values())).T
    cols = list(attr.keys())
    dataframe.columns = cols
    dataframe.insert(0, 'id', data['id'])
    dataframe.insert(1, 'type', data['type'])
    measurement = "nervos_hashrate"
    df_2_dict = dataframe.T.to_dict()[0] 
    times = False
    tags = {}
    fields = {}
    fields['current_epoch_average_block_time'] = float(df_2_dict['average_block_time'])
    fields['current_epoch_difficulty'] = int(df_2_dict['current_epoch_difficulty'])
    fields['hash_rate'] = float(df_2_dict['hash_rate'])
    fields['id'] = int(df_2_dict['id'])
    fields['tip_block_time'] = int(df_2_dict['tip_block_number'])
    fields['type'] = df_2_dict['type']
    fields.update({"is_api_return_timestamp": False})
    db.write_points_to_measurement(measurement,times,tags,fields) 
    return 


try:
    blockchain(link)
except:
    error = traceback.format_exc()
    #logger("nervos_hashrate", error)
while True:
    time.sleep(60*10)
    try:
        blockchain(link)
    except:
        error = traceback.format_exc()
        #logger("nervos_hashrate", error)
    

    




