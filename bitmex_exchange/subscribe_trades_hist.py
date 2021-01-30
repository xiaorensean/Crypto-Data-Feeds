import pandas as pd
import os
import sys
import gzip
import requests
import datetime
import time
import urllib.request
import random
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.bitmex_influxdb_client_v1 import InfluxClient




host_1qa = InfluxClient()

def checkInUpdateTime():
    utcnow = datetime.datetime.utcnow()
    seconds_left = (utcnow - utcnow.replace(hour=6, minute=0, second=0, microsecond=0)).total_seconds()
    if int(seconds_left) == 0:
        run_script = True
        time.sleep(1)
    else:
        run_script = False
    return  run_script



def get_file_names(period):
    utc_current = datetime.datetime.utcnow()
    file_name = str(utc_current).split(" ")[0].replace("-","") + '.csv.gz'
    file_names = []
    for idx in range(period):
        dt = utc_current - datetime.timedelta(idx)
        file_name = str(dt).split(" ")[0].replace("-","") + '.csv.gz'
        file_names.append(file_name)
    return file_names


def download_files(file_name):
    data_url = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}".format(file_name)
    response = requests.get(data_url)
    with open(file_name, "wb") as code:
        code.write(response.content)
    urllib.request.urlretrieve(data_url, file_name)


def read_data(file_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    fn = current_dir + "/{}".format(file_name)
    with gzip.open(fn) as f:
        data = pd.read_csv(f)
    df = data
    data_dict = df.T.to_dict()
    data_clean = []
    for idx in range(df.shape[0]):
        data_clean.append(data_dict[idx])
    return data_clean

def write_data(data):
    measurement = "bitmex_trades"
    count = 0
    for d in data:
        count += 1
        fields = {k:d[k] for k in d if k != "symbol" and k != "timestamp"}
        for key, value in fields.items():
            if type(value) == int:
                fields[key] = float(value)
        tags = {}
        tags.update({"symbol":d["symbol"]})
        ts = d["timestamp"].replace("D"," ")[:-3]
        #try:
        dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
        #except:
        #    dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        fields.update({"is_api_return_timestamp": True})
        utc_ts = int(time.mktime(dt_temp.timetuple())) * 1000 + random.random()
        print(utc_ts)
        dbtime = datetime.datetime.utcfromtimestamp(utc_ts/1000)
        print(dbtime)
        print(fields)
        try:
            host_1qa.write_points_to_measurement(measurement,dbtime,tags,fields)
        except:
            fields.update({"side":None})
            try:
                host_1qa.write_points_to_measurement(measurement,dbtime,tags,fields)
            except:
                pass
    print(count)

def backfill_data():
    file_names = get_file_names(366)[1:]
    for fn in file_names:
        print(fn)
        download_files(fn)
        data = read_data(fn)
        write_data(data)

# function to check the latest data
def current_data():
    file_name = get_file_names(2)[-1]
    print(file_name)
    download_files(file_name)
    data = read_data(file_name)
    write_data(data)

if __name__ == "__main__":
    #backfill_data()
    while True:
        if checkInUpdateTime():
            current_data()
        else:
            pass
