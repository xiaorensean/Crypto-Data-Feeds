import traceback
import requests
import time
import pandas as pd
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utils
db = Writer()
all_files = [file for file in os.listdir(current_dir+'/defi_hist') if ".csv" in file]
print(all_files)

def write_data(data,file_name):
    measurement = "defi_total_value_locked"
    for d in data:
        fields = {}
        fields.update({"total_value_locked_usd":d['total_value_locked']})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":file_name.split(".")[0].split("_")[-1]})
        dbtime = d['time']
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


for af in all_files:
    data = pd.read_csv(current_dir+'/defi_hist/'+af)
    data['total_value_locked'] = [float(i.split("$")[1].replace(",","")) for i in data["TVL (USD)"].tolist()]
    data_dict = [data.T.to_dict()[i] for i in data.T.to_dict()]
    print(data_dict)
    write_data(data_dict,af)

