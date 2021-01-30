'''
Subscribes to all metadata for options, 
including Open Interest, Funding Rate, Volume, etc
For the sake of speed, each ticker-channel endpoint is streamed through
an individual websocket in a new process.
'''

import os 
import sys 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata
import multiprocessing
import json
from time import sleep

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.deribit_influxdb_client import DeribitInfluxClient

tickers = [
  "ETH"
]
endpoints = [
  "ticker",
]

def create_process(client, endpoint):
  msg = \
    {"jsonrpc": "2.0",
     "method": "public/subscribe",
     "id": 42,
     "params": {
        "channels": [endpoint]
      }
    }
  process = multiprocessing.Process(target=metadata.call_options_api, args=(client, json.dumps(msg)))
  process.start()
  with open("options_children.txt", "a+") as f:
    lines = ['----------\n', endpoint + "\n", str(process.pid) + "\n", '----------\n']
    f.writelines(lines)
  entry = {
    "endpoint": endpoint,
    "process": process
  }
  return entry

if __name__ == "__main__":
  open("options_children.txt", 'w').close()
  client = DeribitInfluxClient()
  endpoints = metadata.get_options_endpoints(tickers, endpoints)
  processes = []
  for endpoint in endpoints:
    processes.append(create_process(client, endpoint))
    sleep(10)

  while (True):
    for entry in processes:
      process = entry["process"]
      endpoint = entry["endpoint"]
      if not process.is_alive():
        entry["process"].join()
        processes.remove(entry)
        processes.append(create_process(client,endpoint))
    sleep(10)

