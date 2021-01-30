'''
Subscribes to trade channel for all active tickers. 
Don't usually use this script; cryptofeed typically performs better
'''

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import bitmex_connector.metadata_functions as metadata
import multiprocessing
from time import sleep

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.bitmex_influxdb_client import BitmexInfluxClient

endpoints = []

trade_tickers = []
for item in metadata.get_symbols():
	trade_tickers.append(item["symbol"])

for ticker in trade_tickers:
	endpoints.append('trade:' + ticker)

def create_process(client, endpoint):
	process = multiprocessing.Process(target=metadata.subscribe_slow, args=(client, endpoint))
	process.start()
	with open("trade_children.txt", "a+") as f:
		lines = ['----------\n', endpoint + "\n", str(process.pid) + "\n", '----------\n']
		f.writelines(lines)
	entry = {
		"endpoint": endpoint,
		"process": process
	}
	return entry


'''
Script
'''

if __name__ == "__main__":
	open("trade_children.txt", 'w').close()
	client = BitmexInfluxClient()

	processes = []
	for endpoint in endpoints:
		print(endpoint)
		processes.append(create_process(client, endpoint))
		sleep(1)

	while (True):
		for entry in processes:
			process = entry["process"]
			endpoint = entry["endpoint"]
			if not process.is_alive():
				entry["process"].join()
				processes.remove(entry)
				processes.append(create_process(client,endpoint))
		sleep(10)












