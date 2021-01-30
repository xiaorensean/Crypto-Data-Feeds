'''
Uses REST API to continually query liquidations for all perpetual swap instruments
**Sometimes throws errors, but usually no data is lost
'''
import time
import requests
import sys
from os import path
current_dir = path.dirname(path.abspath(__file__))
sys.path.append(current_dir)
import okex_api.swap_api as swap
from time import sleep
from termcolor import colored
pkg_dir = path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.okex_influxdb_client import OkexInfluxClient

# function to automatical fet all tickers
def get_swap_tickers(base_token):
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "perpetual/pc/public/contracts/tickers?type={}".format(base_token)
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['data']
    return data


client = OkexInfluxClient({})

api_key = ''
seceret_key = ''
passphrase = ''



LIMIT = 250
def get_liquidation(client, last_liquidations):
	swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)
	for ticker in tickers:
		new_orders = []
		old_orders = last_liquidations[ticker]
		result = swapAPI.get_liquidation(ticker, '1', '', '', '')
		for order in result:
			if order in old_orders:
				continue
			else:
				new_orders.append(order)
				old_orders.append(order)
				if (len(old_orders) > LIMIT):
					old_orders.pop(0)
		old_orders.sort(key = lambda x:x["created_at"])
		new_orders.sort(key = lambda x:x["created_at"])

		client.write_liquidation(new_orders)

	return last_liquidations


def get_liquidations(client):
    swap_data = get_swap_tickers("USD") + get_swap_tickers("USDT")
    tickers = [sd['contract'] for sd in swap_data]
    swapAPI = swap.SwapAPI(api_key, seceret_key, passphrase, True)
    all_data = []
    for ticker in tickers:
        all_data += swapAPI.get_liquidation(ticker, '1', '', '', '')

    client.write_liquidation(all_data)    


if __name__ == "__main__":
    get_liquidations(client)
    while (True):
        time.sleep(60*60)
        try:
            get_liquidations(client)
        except:
            try:
                get_liquidations(client)
            except:
                client.send_email("Subject: {}\n\n{}".format("ERROR with Script", "okex/subscribe_liquidation.py"))



