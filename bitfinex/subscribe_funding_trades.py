import traceback
from datetime import datetime
import time
import multiprocessing as mp
import math
import copy
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bitfinex_api.BfxRest import BITFINEXCLIENT
from utils import get_all_tickers

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_client_v1 import InfluxClient


# utils
def get_date(ts):
    return datetime.utcfromtimestamp(ts / 1000)


def roundup(x):
    return int(math.ceil(x / 1000) * 1000)


# measurement
measurement = 'bitfinex_funding_trade'

# bitfinex client
API_KEY = "API_KEY"
API_SECRETE = "API_SECRETE"
bitfinex = BITFINEXCLIENT(API_KEY, API_SECRETE)
influxdb = InfluxClient()


# collect hsitory funding trades
def get_funding_trade(start_ts, ticker, measurement):
    get_all_data = False
    current_ts = roundup(time.time() * 1000)
    last_ts_temp = 0
    # collect all historical trade
    while not get_all_data:
        end_ts = current_ts
        # api call
        time.sleep(3)
        funding_trades = bitfinex.get_public_trades(ticker, start_ts, end_ts)
        if len(funding_trades) == 0:
            break
        # wrtie to database
        for t in funding_trades:
            data_entry = {'ID': 0, 'MTS': 0, 'AMOUNT': 0.0, 'RATE': 0.0, 'PERIOD': 0}
            for idx in range(len(t)):
                keys = list(data_entry.keys())
                if keys[idx] == 'ID':
                    data_entry[keys[idx]] = int(t[idx])
                if keys[idx] == 'MTS':
                    data_entry[keys[idx]] = int(t[idx])
                if keys[idx] == 'AMOUNT':
                    data_entry[keys[idx]] = float(t[idx])
                if keys[idx] == 'RATE':
                    data_entry[keys[idx]] = float(t[idx])
                if keys[idx] == 'PERIOD':
                    data_entry[keys[idx]] = int(t[idx])
            times = False
            tags = {}
            tags.update({'SYMBOL': ticker})
            fields = copy.copy(data_entry)
            fields.update({"is_api_return_timestamp": True})
            influxdb.write_points_to_measurement(measurement, times, tags, fields)
        # get the timestamp of very last data entry
        last_ts = funding_trades[-1][1]
        print(ticker, 'start ', start_ts)
        print(ticker, 'end ', end_ts)
        # checking 1: timestamp of the very last data entry exceeds the current timesttamp
        # checking 2: timestamp for last data entry is not updating
        if last_ts >= end_ts or last_ts_temp == last_ts:
            get_all_data = True
        else:
            # assign start timestamp as timestamp of the very last data entry
            start_ts = last_ts
            last_ts_temp = last_ts


# check for the most recent data MTS
def reset_start_ts(symb):
    data = influxdb.query_tables(measurement, ["MTS", "WHERE SYMBOL = '{}' ORDER BY time DESC LIMIT 1".format(symb)])
    start_ts = data.MTS.tolist()[0]
    return start_ts


funding_tickers = get_all_tickers('funding')


# check for updating data or backup historical data
def start_ts_check(symb):
    try:
        symb_in_db = influxdb.get_tag_values(measurement, 'SYMBOL')
    except IndexError:
        symb_in_db = []
    if symb in symb_in_db:
        start_ts = reset_start_ts(symb)
    else:
        start_ts = 1171265177000
    return start_ts


# collecting data
def collecting(symb):
    try:
        start_ts = start_ts_check(symb)
        get_funding_trade(start_ts, symb, measurement)
    except Exception:
        error_message = traceback.format_exc()
        if "invalid field format" in error_message:
            pass
        else:
            logger(measurement, error_message, symb)
    while True:
        time.sleep(60 * 60)
        try:
            start_ts = start_ts_check(symb)
            get_funding_trade(start_ts, symb, measurement)
        except Exception:
            error_message = traceback.format_exc()
            if "invalid field format" in error_message:
                pass
            else:
                logger(measurement, error_message, symb)

if __name__ == "__main__":
    # collecting all tickers
    for symb in funding_tickers:
        collecting_symb = mp.Process(target=collecting, args=(symb,))
        collecting_symb.start()
