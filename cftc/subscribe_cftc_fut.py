import os
import sys
import pandas as pd
import datetime
import time
import urllib.request
import zipfile, shutil
import traceback

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.bitmex_influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

host_1 = InfluxClient()



measurement = "cftc_futures_report"


def download_files(file_name):
    data_url = "https://www.cftc.gov/files/dea/history/{}".format(file_name)
    with urllib.request.urlopen(data_url) as response, open(file_name, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
        with zipfile.ZipFile(file_name) as zf:
            zf.extractall()

def fields_data_clean(df_dict):
    fields = {}
    for i in df_dict:
        if str(df_dict[i]) == "nan":
            fields.update({i:None})
        else:
            fields.update({i:df_dict[i]})
    return fields

def read_data(file_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    fn = current_dir + "/{}".format(file_name)
    df = pd.read_csv(file_name, compression='zip', header=0, sep=',', quotechar='"')
    #with gzip.open(fn) as f:
    #    data = pd.read_csv(f)
    #df = data
    data_dict = df.T.to_dict()
    data_clean = []
    for idx in range(df.shape[0]):
        data_clean.append(data_dict[idx])
    return data_clean



def write_cftc_report(df_dict,host,measurement): 
    for data in df_dict:
        # filter for tags and time
        fields_raw = {dd:data[dd] for dd in data if \
                      dd != "CFTC_Contract_Market_Code" and \
                      dd != "Report_Date_as_MM_DD_YYYY"}
        fields = fields_data_clean(fields_raw)
        fields.update({"is_api_return_timestamp": True})
        tags = {} 
        tags.update({"CFTC_Contract_Market_Code":data["CFTC_Contract_Market_Code"]})
        dbtime = datetime.datetime.strptime(str(data['Report_Date_as_MM_DD_YYYY']),"%Y-%m-%d %H:%M:%S")
        host.write_points_to_measurement(measurement, dbtime, tags, fields)

def subscribe_cftc_reports(host,measurement):
    # file name
    current_year = str(datetime.datetime.utcnow())[:4]
    file_name = "dea_fut_xls_{}.zip".format(current_year)
    #file_name = "deacot{}.zip".format(current_year)
    download_files(file_name)
    data = read_data(file_name)
    write_cftc_report(data,host,measurement)


if __name__ == "__main__":
    try:
        subscribe_cftc_reports(host_1, measurement)
    except:
        error = traceback.format_exc()
        #logger(measurement,error)
    while True:
        time.sleep(60*60*7)
        try:
            subscribe_cftc_reports(host_1, measurement)
        except:
            error = traceback.format_exc()
            #logger(measurement, error)

