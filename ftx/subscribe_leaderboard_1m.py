import traceback
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from ftx_api.FtxRest import get_leaderboard
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger
import time 
import datetime

db = InfluxClient()

measurement = "ftx_leaderboard_1m"

def write_data(resp,data_type,measurement):
    current_timestamp = int(time.time()*1000)
    data = resp['result']
    for idx,d in enumerate(data):
        fields = {}
        fields.update({"rank":int(idx+1)})
        fields.update({"isRealName":str(d["isRealName"])})
        fields.update({"name":d["name"]})
        fields.update({"current_timestamp":current_timestamp})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"type":data_type})
        dbtime = False
        db.write_points_to_measurement(measurement, dbtime, tags, fields)        

def write_all_data(measurement):
    param_vol = "volume"
    volume = get_leaderboard(param_vol)
    write_data(volume,param_vol,measurement)
    param_maker_vol = "maker_volume"
    maker_volume = get_leaderboard(param_vol)
    write_data(maker_volume,param_maker_vol,measurement)
    param_pnl = "pnl"
    pnl = get_leaderboard(param_pnl)
    write_data(pnl,param_pnl,measurement)


def subscribe_leaderbaord(measurement):
    try:
        write_all_data(measurement)
    except Exception:
        error = traceback.format_exc()
        logger(measurement, error)
    while True:
        time.sleep(57)
        try:
            write_all_data(measurement)
        except Exception:
            error = traceback.format_exc()
            logger(measurement, error)


if __name__ == "__main__":
    subscribe_leaderbaord(measurement)