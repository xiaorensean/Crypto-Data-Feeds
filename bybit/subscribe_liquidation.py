import traceback
import os 
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bybit_api.bybitRestApi import liquidation_order, tickers_info

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger
from utility.timestamp_index_converter import unique_time_index_tranform


db = InfluxClient()

measurement = "bybit_liquidation"


def write_liquidation_data(symb,measurement):
    # fetch api data
    data = liquidation_order(symb,'1550196497272')['result'] 
    # set unique time index
    data_clean = unique_time_index_tranform(data)
    # write data to database
    for dc in data_clean:
        fields = {}
        fields.update({"id":int(dc['id'])})
        fields.update({"price":float(dc['price'])})
        fields.update({"qty":float(dc['qty'])})
        fields.update({"side":dc['side']})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol":symb})
        dbtime = datetime.datetime.utcfromtimestamp(dc['time']/1000)
        db.write_points_to_measurement(measurement, dbtime, tags, fields)


def subscribe_liquidation_data(measurement):
    tickers = [ti['symbol'] for ti in tickers_info()]
    for symb in tickers:
        try:
            write_liquidation_data(symb, measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement,error,symb)


if __name__ == "__main__":
    subscribe_liquidation_data(measurement)
    while True:
        time.sleep(60*5)
        subscribe_liquidation_data(measurement)







