import traceback
import time
import datetime
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utility
db = Writer()

measurement = "poloniex_leaderboard"

def subscribe_poloniex_leaderboard(measurement):
    url = "https://x-api.poloniex.com/v1/promos/leaderboard/contest/trc20-usdt-deposit-competition-2020"
    headers = {'accept': 'application/json, text/plain, */*',
           'accept-encoding': 'gzip, deflate, br',
           'accept-language': 'en-US,en;q=0.9',
           'origin': 'https://poloniex.com',
           'referer': 'https://poloniex.com/leaderboard/trc20-usdt-deposit-competition-2020',
           'sec-fetch-mode': 'cors',
           'sec-fetch-site': 'same-site',
           'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
           'x-app-id': 'poloniex-web',
           'x-requested-with': 'XMLHttpRequest'} 
    response = requests.get(url,headers=headers)
    resp = response.json()
    if list(resp.keys())[0] == 'response':
        data = resp['response']['data']
    else:
        data = None
        print('error')
    ts = " ".join(data['date'].split("Z")[0].split("T"))
    dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
    uts = int(time.mktime(dt_temp.timetuple()) * 1000)
    current_snapshot = uts
    leaderboard_data = data['leaderBoard']
    fields = {}
    for ld in leaderboard_data:
        fields.update({"ref_ts":current_snapshot})
        fields.update({"rank":ld["rank"]})
        fields.update({"rank_change":ld["rankChange"]})
        fields.update({"account":ld["userId"]})
        fields.update({"net_deposit":ld["score"]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)


# start collecting for funding rates data
if __name__ == "__main__":
    try:
        subscribe_poloniex_leaderboard(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
        time.sleep(30)
        subscribe_poloniex_leaderboard(measurement)
    while True:
        # set up scheduler for 1 hour
        time.sleep(60*60)
        try:
            subscribe_poloniex_leaderboard(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            time.sleep(30)
            subscribe_poloniex_leaderboard(measurement)

            
    
    
