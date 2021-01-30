import traceback
import time
import datetime
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from curvefi_api.api_client import get_hist_data, get_pool
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utils
db = Writer()
pool_list = ["compound", "usdt", "y", "busd", "susd", "pax", "ren2", "rens"]
print(pool_list)
freq_list = ["1", "5", "10", "30", "1440"]

def write_data(pool):
    measurement = "curve_pool_data"
    coin_precision= 10**18
    data = get_hist_data(pool, "1")
    for d in data:
        fields = {}
        fields.update({"fee":d["fee"]/coin_precision})
        fields.update({"admin_fee":d["admin_fee"]})
        fields.update({"supply":d["supply"]/coin_precision})
        fields.update({"virtual_price":d["virtual_price"]/coin_precision})
        for idx in range(len(d["balances"])):
            fields.update({"balances"+str(idx):d["balances"][idx]/coin_precision})
        for idx in range(len(d["rates"])):
            fields.update({"rates" + str(idx): d["rates"][idx]/coin_precision})
        if len(d['volume']) != 0:
            for v in d['volume']:
                fields.update({"volume"+"_"+v+"_sold":d["volume"][v][0]/coin_precision})
                fields.update({"volume" + "_" + v + "_bought": d["volume"][v][0]/coin_precision})
        if len(d["prices"]) != 0:
            for p in d["prices"]:
                fields.update({p+"_open":d["prices"][p][0]})
                fields.update({p+"_high": d["prices"][p][1]})
                fields.update({p+"_low": d["prices"][p][2]})
                fields.update({p+"_close": d["prices"][p][3]})
        tags = {}
        tags.update({"symbol":pool})
        dbtime = datetime.datetime.utcfromtimestamp(d["timestamp"])
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
        print(fields)

def subscribe_curve_data():
    pool_list = ["compound", "usdt", "y", "busd", "susd", "pax", "ren2", "rens"]
    for pl in pool_list:
        write_data(pl)
    while True:
        time.sleep(60*60*24)
        for pl in pool_list:
            write_data(pl)

if __name__ == "__main__":
    subscribe_curve_data()
