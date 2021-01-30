'''
Subscribes to all channels which send info on a less regular basis, such as 
funding and liquidation
For the sake of speed, the script spawns a separate process and websocket for each endpoint
'''


import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import bitmex_connector.metadata_functions as metadata
import multiprocessing
from time import sleep
from termcolor import colored

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.bitmex_influxdb_client import BitmexInfluxClient

endpoints = [
		'funding',
		'liquidation',
		'settlement',
		'insurance',
	        'instrument'
	]

def create_process(client, endpoint):
	process = multiprocessing.Process(target=metadata.subscribe_slow, args=(client, endpoint))
	process.start()
	with open("slow_children.txt", "a+") as f:
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
	open("slow_children.txt", 'w').close()
	client = BitmexInfluxClient()

	processes = []
	for endpoint in endpoints:
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
				print(colored(endpoint, "red"))
		sleep(10)












