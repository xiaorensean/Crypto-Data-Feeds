import numpy as np
import datetime
import requests
import time
import traceback

# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from okex_api.swap_api import SwapAPI
from subscribe_oi_swaps import get_swap_tickers
from subscribe_orderbook_futures_usd1 import write_to_influxdb
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()

okex_swap = SwapAPI('', '', '')

measurement = "okex_orderbook"


def subscribe_orderbook_swap_usdt():
    swap_tickers_usd = [i['contract'] for i in get_swap_tickers("USDT")]
    for symb in swap_tickers_usd:
        try:
            data = okex_swap.get_depth(symb, 200)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error,symb)
            return 
        write_to_influxdb('bids', measurement, data, symb)
        write_to_influxdb('asks', measurement, data, symb)

          
if __name__ == '__main__':
    subscribe_orderbook_swap_usdt()
    while True:
        time.sleep(35)
        subscribe_orderbook_swap_usdt()


