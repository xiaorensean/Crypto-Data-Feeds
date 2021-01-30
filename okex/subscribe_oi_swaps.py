'''
Uses REST API to continually query open interest for all perpetual swap instruments
**Sometimes throws errors, but usually no data is lost
'''
import traceback
import requests
import sys
from os import path
import time
current_dir = path.dirname( path.abspath(__file__) )
sys.path.append(current_dir)
import okex_api.swap_api as swap
from termcolor import colored

pkg_dir = path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.okex_influxdb_client import OkexInfluxClient
from utility.error_logger_writer import logger


api_key = ''
seceret_key = ''
passphrase = ''

# function to automatical fet all tickers
def get_swap_tickers(base_token):
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "perpetual/pc/public/contracts/tickers?type={}".format(base_token)
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['data']
    return data


def get_open_interest(client):
    swap_data = get_swap_tickers("USD") + get_swap_tickers("USDT")
    tickers = [sd['contract'] for sd in swap_data]
    swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)
    for ticker in tickers:
        result = swapAPI.get_holds(ticker)
        time.sleep(1)
        if not client.write_open_interest(result):
            print(colored("Database error!", "yellow"))
            client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "okex/subscribe_swap_oi.py"))


if __name__ == "__main__":
    client = OkexInfluxClient({})
    get_open_interest(client)
    while (True):
        time.sleep(60*5)
        try:
            get_open_interest(client)
        except Exception:
            error_message = traceback.format_exc()
            #logger("okex_swap_oi",error_message)
