'''
Subscribes to all channels for all futures and swaps, 
including ticker info, trades, price, etc.
For the sake of speed, each ticker-channel endpoint is streamed through
an individual websocket in a new process.
'''

import requests
import multiprocessing
from time import sleep
import dateutil.parser as dp

import sys
import os
from os import path
current_dir = path.dirname(path.abspath(__file__))
sys.path.append(current_dir)
import okex_connectors.metadata_functions as metadata

outer_dir = os.path.dirname(current_dir)
sys.path.append(outer_dir)
from influxdb_client.okex_influxdb_client import OkexInfluxClient


def get_tickers():
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "futures/pc/market/marketOverview.do?symbol=f_usd_all"
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['ticker']
    all_tickers = list(set([d['symbolName'] for d in data]))
    symbol_usdt = [i.upper()+"-USD-" for i in all_tickers]
    symbol_usd = [i.upper()+"-USDT-" for i in all_tickers]
    pairs = symbol_usd + symbol_usdt
    return pairs



endpoints = [
    "/ticker",
    "/price_range",
    "/mark_price",
#    "/trade",
]

pairs = get_tickers()
#pairs = ["BTC-USD-","BTC-USDT-","ETH-USD-","ETH-USDT-",
#           "ETC-USD-","ETC-USDT-","BCH-USD-","BCH-USDT-",
#           "BSV-USD-","BSV-USDT-"]

api_key = ''
seceret_key = ''
passphrase = ''


def create_process(client, channel):
    entries = []
    for endpoint in channel["endpoints"]:
        process = multiprocessing.Process(target=metadata.subscribe, args=(client, channel, endpoint))
        process.start()
        with open("children.txt", "a+") as f:
            lines = ['----------\n', endpoint + "\n", str(process.pid) + "\n", '----------\n']
            f.writelines(lines)
        entry = {
            "endpoint": endpoint,
            "process": process,
            "channel": channel
        }
        entries.append(entry)
    return entries

def create_singular_process(client, channel, endpoint):
    process = multiprocessing.Process(target=metadata.subscribe, args=(client, channel, endpoint))
    process.start()
    with open("children.txt", "a+") as f:
        lines = ['----------\n', endpoint + "\n", str(process.pid) + "\n", '----------\n']
        f.writelines(lines)
    entry = {
        "endpoint": endpoint,
        "process": process,
        "channel": channel
    }
    return entry

def get_all_new_processes(channels):
    new_processes = []
    for channel in channels:
        entries = create_process(client, channel)
        for entry in entries:
            new_processes.append(entry)
        sleep(1)
    return new_processes



if __name__ == "__main__":
    open("children.txt", 'w').close()
    next_switch_time = metadata.get_next_expiry_time()

    channels = metadata.get_swap_endpoints(pairs, endpoints, metadata.get_all_suffixes())
    client = OkexInfluxClient(channels)
    processes = get_all_new_processes(channels)

    while (True):
        try:
            if dp.parse(metadata.get_server_time()) > next_switch_time:
                channels = metadata.get_swap_endpoints(pairs, endpoints, metadata.get_all_suffixes())
                client = OkexInfluxClient(channels)
                processes = get_all_new_processes(channels)
                next_switch_time = metadata.get_next_expiry_time()
        except:
            continue
        for entry in processes:
            process = entry["process"]
            endpoint = entry["endpoint"]
            channel = entry["channel"]
            if not process.is_alive():
                entry["process"].join()
                processes.remove(entry)
                processes.append(create_singular_process(client, channel, endpoint))
        sleep(1)

