import traceback
import requests
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir) 
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utils 
db = Writer()

def subscribe_leaderboard(measurement):
    url = "https://api.compound.finance/api/v2/governance/accounts?page_size=100&page_number=1&with_history=false&network=mainnet"
    response = requests.get(url)
    data = response.json()
    pg_info = data['pagination_summary']['total_pages']
    for i in range(1,pg_info+1):
        url = "https://api.compound.finance/api/v2/governance/accounts?page_size=100&page_number={}&with_history=false&network=mainnet".format(i)
        response = requests.get(url)
        data = response.json()
        ld_data = data['accounts']
        write_leaderboard_data(ld_data,measurement)


def write_leaderboard_data(data,measurement):
    current_snapshot = int(time.time()*1000)
    for d in data:
        fields = {}
        fields.update({"rank":d['rank']})
        fields.update({"name":d['display_name']})
        fields.update({"vote_weight":float(d["vote_weight"])})
        fields.update({"vote":float(d["votes"])})
        fields.update({"proposals_created":int(d['proposals_created'])})
        fields.update({"account_url":d["account_url"]})
        fields.update({"address":d["address"]})
        fields.update({"balance":float(d["balance"])})
        fields.update({"ref_ts":current_snapshot})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement, dbtime, tags, fields)
    

if __name__ == '__main__':
    measurement = "compound_leaderboard"
    try:
        subscribe_leaderboard(measurement)
    except Exception:
        error = traceback.format_exc()
        #logger(measurement,error)
    while (True):
        time.sleep(60*60*12)
        try:
            subscribe_leaderboard(measurement)
        except Exception:
            pass
            #logger(measurement,error)

