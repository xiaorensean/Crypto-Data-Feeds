import traceback
import time
import datetime
import random
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from namebase_api.namebaseApi import get_trade
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "namebase_trades"
symbol = "HNSBTC"


def init_collection_trade(symbol,mesurement):
    current_time = str(int(time.time()*1000))
    data = get_trade(symbol,current_time)
    fields = {}
    for d in data:
        fields.update({"isBuyerMaker":d["isBuyerMaker"]})
        fields.update({"price":float(d["price"])})
        fields.update({"quantity":float(d["quantity"])})
        fields.update({"quoteQuantity":float(d["quoteQuantity"])})
        fields.update({"tradeId":int(d["tradeId"])})
        fields.update({"is_api_return_timestamp": True})
        dbtime = datetime.datetime.utcfromtimestamp((int(d["createdAt"])+random.random())/1000)
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_trade(symbol,mesurement):
    current_time = str(int(time.time()*1000))
    data_api = get_trade(symbol,current_time)
    for d in data_api:
        fields = {}
        fields.update({"isBuyerMaker":d["isBuyerMaker"]})
        fields.update({"price":float(d["price"])})
        fields.update({"quantity":float(d["quantity"])})
        fields.update({"quoteQuantity":float(d["quoteQuantity"])})
        fields.update({"tradeId":int(d["tradeId"])})
        fields.update({"is_api_return_timestamp": True})
        dbtime = datetime.datetime.utcfromtimestamp((int(d["createdAt"])+random.random())/1000)
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
    else:
        pass


if __name__ == "__main__":
    try:
        subscribe_trade(symbol,measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,symbol)
    while True:
        time.sleep(60*5)
        try:
            subscribe_trade(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,symbol)

