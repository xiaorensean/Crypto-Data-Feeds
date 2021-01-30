import traceback
import time
import requests
import json
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger


# utility
db = Writer()

def get_leaderboard(freq,type):
    url = "https://www.binance.com/gateway-api/v1/public/future/leaderboard/getLeaderboardRank"
    param = {"periodType": freq, "statisticsType": type, "tradeType": "PERPETUAL"}
    headers = {"content-type": "application/json",
               "cookie": "cid=qZJplR5C; __zlcmid=w0ingK86iMBquA; isHideHeadLangSeclet=true; defaultAssetTab=BNB; h-exchange-20202028=true; _ga=GA1.2.900106848.1586367080; lang=en; userPreferredCurrency=USD_USD; exp=tradeng; exp-random=0.08203077124182734; expFut=futuresng; theme=DARK; defaultAssetTab=BTC; bnc-uuid=4d55a785-5733-41f9-9999-d8d0f4176227; _gid=GA1.2.339475170.1596678542; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216f580dba3c118-041a0bd6cea263-1d376b5b-1764000-16f580dba3dd11%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%2216f580dba3c118-041a0bd6cea263-1d376b5b-1764000-16f580dba3dd11%22%7D; lang=en; _gat=1",
               "csrftoken": "d41d8cd98f00b204e9800998ecf8427e"}
    response = requests.post(url,data=json.dumps(param), headers=headers)
    data = response.json()['data']
    return data

freqs = ["DAILY", "WEEKLY", "MONTHLY"]
type = ["ROI","PNL"]

def write_data(freq,type):
    data = get_leaderboard(freq,type)
    current_time = time.time()
    measurement = "binance_leaderboard"
    for d in data:
        fields = {}
        try:
            fields.update({'futureUid':int(d["futureUid"])})
        except:
            fields.update({'futureUid': None})
        fields.update({'nickName':d["nickName"]})
        fields.update({'userPhotoUrl':d["userPhotoUrl"]})
        fields.update({"rank":int(d["rank"])})
        fields.update({"value":float(d["value"])})
        fields.update({"positionShared":d["positionShared"]})
        fields.update({"is_api_return_timestamp":False})
        fields.update({"ref_ts":int(current_time)})
        tags = {}
        tags.update({"leaderboard_type":type})
        tags.update({"frequency":freq})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_leaderboard():
    freqs = ["DAILY", "WEEKLY", "MONTHLY"]
    type = ["ROI", "PNL"]
    for freq in freqs:
        for t in type:
            try:
                write_data(freq,t)
            except:
                pass
    while True:
        time.sleep(60*60*12)
        for freq in freqs:
            for t in type:
                try:
                    write_data(freq, t)
                except:
                    pass

if __name__ == "__main__":
    subscribe_leaderboard()

