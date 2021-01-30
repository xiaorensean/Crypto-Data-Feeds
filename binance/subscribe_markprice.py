import traceback
import websocket
import json
import datetime
import time
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from influxdb_client.bitmex_influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

# utility
db = InfluxClient()

# write mark price data
def write_markprice(data_all_symbols):
    measurement = "binance_markprice"
    for data in data_all_symbols:
        symbol = data['s']
        print(symbol)
        try:
            data_db = db.query_tables(measurement,["*","where symbol = '{}' order by time desc limit 1".format(symbol)],"raw")[0][0]
        except:
            data_db = None
        if data_db is not None:
            if float(data['p']) == data_db['mark_price']:
                print(data['p'])
                print(data_db['mark_price'])
                print(symbol,"price is the same")
                pass
            else:
                fields = {}
                fields.update({"mark_price": float(data['p'])})
                fields.update({"funding_rate": float(data['r'])})
                fields.update({"next_funding_time": int(data['T'])})
                fields.update({"is_api_return_timestamp": True})
                tags = {}
                tags.update({"symbol": data["s"]})
                db_time = datetime.datetime.utcfromtimestamp(data['E'] / 1000)
                db.write_points_to_measurement(measurement, db_time, tags, fields)
        else:
            fields = {}
            fields.update({"mark_price": float(data['p'])})
            fields.update({"funding_rate": float(data['r'])})
            fields.update({"next_funding_time": int(data['T'])})
            fields.update({"is_api_return_timestamp": True})
            tags = {}
            tags.update({"symbol": data["s"]})
            db_time = datetime.datetime.utcfromtimestamp(data['E'] / 1000)
            db.write_points_to_measurement(measurement, db_time, tags, fields)

def ws_markprice():
    endpoint = 'wss://fstream.binance.com/stream'
    ws = websocket.create_connection(endpoint)
    msg = {
        "method":"SUBSCRIBE",
        "params":["!markPrice@arr"],
        "id":10
          }
    ws.send(json.dumps(msg))
    # subscribing successfully
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    # receiving data
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    markprice = data['data']
    print(markprice)
    write_markprice(markprice)


def subscribe_markprice():
    try:
        ws_markprice()
    except:
        measurement = "binance_markprice"
        error = traceback.format_exc()
        print(error)
        #logger(measurement,error)

    while True:
        time.sleep(1)
        try:
            ws_markprice()
        except:
            measurement = "binance_markprice"
            error = traceback.format_exc()
            print(error)
            #logger(measurement, error)

if __name__ == "__main__":
    subscribe_markprice()

