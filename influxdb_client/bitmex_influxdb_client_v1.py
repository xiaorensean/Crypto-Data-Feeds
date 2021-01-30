import requests
import json
from time import sleep
import time
from datetime import datetime
from termcolor import colored
import copy
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from influxdb_client.influxdb_client_v1 import InfluxClient



class BitmexInfluxClient(object):
    
    def __init__(self):
        self.influxdb = InfluxClient()        
    
    # subscribe bitmex data
    def get_bitmex_funding_rates(self,asset_symbol):
        endpoint = 'https://www.bitmex.com/api/v1/instrument?symbol={}&count=100&reverse=false'.format(asset_symbol)
        response = requests.get(endpoint)
        if response.status_code == int(200):
            data = response.text
            json_format = json.loads(data)
        else:
            counter = 0 
            while counter <= 4:
                counter += 1
                sleep(10)
                try:
                    response = requests.get(endpoint)
                except: 
                    pass
                else:
                    data = response.text
                    json_format = json.loads(data)
                    break
        return json_format
    

    def bitmex_funding_rates_update(self,json_format,measurement):
        data_entry = {'timestamp':0,'symbol':0,'fundingRate':0,'indicativeFundingRate':0}
        for cl in data_entry.keys():
            if cl == 'fundingRate' or cl == 'indicativeFundingRate':
                data_entry.update({cl:float(json_format[cl])})
            else:
                data_entry.update({cl:json_format[cl]})
            
        dbtime = data_entry['timestamp']
        tags = {}
        tags.update({"symbol":data_entry["symbol"]})
        fields = copy.copy(data_entry)
        del fields["timestamp"]
        del fields["symbol"]
        fields.update({"is_api_return_timestamp": True})
        self.influxdb.write_points_to_measurement(measurement,dbtime,tags,fields)
    
    
    def subscribe_bitmex_leaderboard(self,options,measurement):
        endpoint = 'https://www.bitmex.com/api/v1/leaderboard?method={}'.format(options)
        current_time = int(time.time()*1000)
        response = requests.get(endpoint)
        data = response.text
        json_format = json.loads(data)
        dbtime = False
        tags = {}
        for idx in range(len(json_format)):
            json_format[idx].update({'ref_ts':current_time})
            json_format[idx].update({'Rank':idx+1})
            fields = json_format[idx]
            fields.update({"is_api_return_timestamp": False})
            self.influxdb.write_points_to_measurement(measurement,dbtime,tags,fields)

    def subscribe_orderbook(self,symbol,depth):
        endpoint = "https://www.bitmex.com/api/v1/orderBook/L2?symbol={}&depth={}".format(symbol,depth)
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.text
            json_data = json.loads(data)
        else:
            print("Error: "+str(response.status_code) + " - " + str(response.text))
            json_data = []
        # write to database 
        measurement = "bitmex_orderbook"
        if len(json_data) == 0:
            return
        else:
            current_utc = time.time()
            dbtime = False
            tags = {"symbol":symbol}
            fields = {}
            for ob in json_data:
                fields.update({"id":int(ob["id"])})
                fields.update({"side":ob["side"]})
                fields.update({"size":int(ob["size"])})
                fields.update({"price":float(ob["price"])})
                fields.update({"ref_ts":int(current_utc)})
                fields.update({"is_api_return_timestamp": False})
                self.influxdb.write_points_to_measurement(measurement,dbtime,tags,fields)
        
        
        

if __name__ == '__main__':
    db = BitmexInfluxClient()
    a = db.get_bitmex_funding_rates('XBTUSD')



    
