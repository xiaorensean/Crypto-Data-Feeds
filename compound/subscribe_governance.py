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

def write_gov_summary(data):
    measurement = "compound_governance_summary"
    fields = {k:float(data[k]) for k in data}
    fields.update({"is_api_return_timestamp": False})
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_gov_markets(data):
    measurement = "compound_governance_market"
    for d in data:
        fields = {k:d[k] for k in d if k != 'symbol'}
        for k in fields:
            try:
                fields.update({k:float(d[k])})
            except:
                fields.update({k:d[k]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":d['symbol']})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_governance_data():
    governance = "https://api.compound.finance/api/v2/governance/comp?network=mainnet"
    response = requests.get(governance)
    data = response.json()
    gov_market = data["markets"]
    gov_summary = {d: data[d] for d in data if d != 'markets' and d != 'request'}
    write_gov_markets(gov_market)
    write_gov_summary(gov_summary)

subscribe_governance_data()

if __name__ == '__main__':
    try:
        subscribe_governance_data()
    except Exception:
        error = traceback.format_exc()
        #logger("compound_governance",error)
    while (True):
        time.sleep(55)
        try:
            subscribe_governance_data()
        except Exception:
            pass
            #logger("compound_governance",error)

