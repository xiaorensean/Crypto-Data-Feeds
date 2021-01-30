import multiprocessing
from time import sleep
from termcolor import colored
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bitmex_connector.metadata_functions import get_symbols
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.bitmex_influxdb_client_v1 import BitmexInfluxClient

bitmex = BitmexInfluxClient()
# get all active symbols
active_symbols = [symb["symbol"] for symb in get_symbols()]


def orderbook_scheduler(ticker,depth,wait):
    bitmex.subscribe_orderbook(ticker,depth)
    while True:
        sleep(wait)
        bitmex.subscribe_orderbook(ticker,depth)
        
def subscribe_tickers(ticker):
    if ticker == "XBTUSD":
        xbt_depth = 200
        xbt_sleep = 30
        orderbook_scheduler(ticker,xbt_depth,xbt_sleep)
    else:
        other_depth = 200
        other_sleep = 60
        orderbook_scheduler(ticker,other_depth,other_sleep)


def create_process(ticker):
	process = multiprocessing.Process(target=subscribe_tickers, args=(ticker,))
	process.start()
	with open("orderbook_children.txt", "a+") as f:
		lines = ['----------\n', ticker + "\n", str(process.pid) + "\n", '----------\n']
		f.writelines(lines)
	entry = {
		"ticker": ticker,
		"process": process
	}
	return entry


if __name__ == "__main__":
	open("orderbook_children.txt", 'w').close()
    
	processes = []
	for ticker in active_symbols:
		processes.append(create_process(ticker))
		sleep(1)

	while (True):
		for entry in processes:
			process = entry["process"]
			ticker = entry["ticker"]
			if not process.is_alive():
				entry["process"].join()
				processes.remove(entry)
				processes.append(create_process(ticker))
				print(colored(ticker, "red"))
		sleep(10)
