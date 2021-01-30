from dateutil import parser
import datetime
import traceback
import os
import sys
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from api.api import get_bitcoin_futures_index

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "cme_futures_index"


def convert_dt(data):
    utc_hour = str(datetime.datetime.utcnow()).split(" ")[1][:2]
    print(utc_hour)
    dt_raw = " ".join(data["updated"].split(" ")[3:]) + " " + utc_hour + data["updated"].split(" ")[0][2:]
    dt = parser.parse(dt_raw)
    return dt


def write_data(measurement):
    data = get_bitcoin_futures_index()
    btc_fut_data = data['quotes']
    for bfd in btc_fut_data:
        print(bfd)
        fields = {}
        try:
            fields.update({"change": float(bfd["change"])})
        except:
            fields.update({"change": None})
        try:
            fields.update({"close": float(bfd["close"])})
        except:
            fields.update({"close": None})
        fields.update({"code": bfd["code"]})
        fields.update({"exchangeCode": bfd["exchangeCode"]})
        fields.update({"expirationDate": int(bfd["expirationDate"])})
        fields.update({"expirationMonth": bfd["expirationMonth"]})
        try:
            fields.update({"high": float(bfd["high"])})
        except:
            fields.update({"high": None})
        try:
            fields.update({"highLimit": float(bfd["highLimit"])})
        except:
            fields.update({"highLimit": None})
        fields.update({"highLowLimits": bfd["highLowLimits"]})
        try:
            fields.update({"last": float(bfd["last"])})
        except:
            fields.update({"last": None})
        try:
            fields.update({"low": float(bfd["low"])})
        except:
            fields.update({"low": None})
        try:
            fields.update({"lowLimit": float(bfd["lowLimit"])})
        except:
            fields.update({"lowLimit": None})
        try:
            fields.update({"open": float(bfd["open"])})
        except:
            fields.update({"open": None})
        try:
            value_raw = bfd["percentageChange"].split("%")[0]
            if value_raw[0] == "+":
                value = float(value_raw[1:])/100
            else:
                value = - float(value_raw[1:])/100
            fields.update({"percentageChange": value})
        except:
            fields.update({"percentageChange": None})
        fields.update({"priorSettle": bfd["priorSettle"]})
        fields.update({"productCode": bfd["productCode"]})
        fields.update({"productId": bfd["productId"]})
        fields.update({"productName": bfd["productName"]})
        try:
            fields.update({"volume": float("".join(bfd["volume"].split(",")))})
        except:
            fields.update({"volume": None})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"symbol": bfd["quoteCode"]})
        dbtime = convert_dt(bfd)
        db.write_points_to_measurement(measurement, dbtime, tags, fields)


def subscribe_data(mesurement):
    try:
        write_data(measurement)
    except Exception:
        error = traceback.format_exc()
        #logger(measurement, error)
    while True:
        time.sleep(60)
        try:
            write_data(measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error)


if __name__ == "__main__":
    subscribe_data(measurement)
    # write_data(measurement)
