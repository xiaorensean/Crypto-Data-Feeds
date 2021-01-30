'''
Subscribe to orderbook for all BTC-based swaps, futures, and options
'''
import multiprocessing
import websocket
import json
#import nest_asyncio
from time import sleep

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata
sys.path.append(os.path.dirname( current_dir ))
from influxdb_client.influxdb_writer import Writer

db = Writer()

tickers = [
  "BTC", 
  "ETH"
]
endpoints = [
  "ticker",
]

def get_tickers():
    swap_ep = metadata.get_swap_ticker()
    future_ep = metadata.get_futures_ticker(tickers, endpoints)
    all_tickers = swap_ep + future_ep 
    return all_tickers

measurement = "deribit_orderbook"


def write_ask_bid(data,side_type,measurement):
    fields = {}
    side_data = data[side_type]
    timestamp = data['timestamp']
    instrument_name = data['instrument_name']
    for sd in side_data:
        fields.update({"price":float(sd[0])})
        fields.update({"amount":float(sd[1])})
        fields.update({"timestamp":timestamp})
        fields.update({"type":side_type})
        tags = {"symbol":instrument_name}
        time = False
        db.write_points_to_measurement(measurement,time,tags,fields)


def write_data(ticker,measurement):
    msg = \
    {
      "jsonrpc" : "2.0",
      "id" : 8772,
      "method" : "public/get_order_book",
      "params" : {
      "instrument_name" : ticker,
      "depth" : 100
                 }
    }

    ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
    ws.send(json.dumps(msg))
    response = ws.recv()
    try:
        orderbook = json.loads(response)['result']
    except:
        pass
    write_ask_bid(orderbook,"asks",measurement)
    write_ask_bid(orderbook,"bids",measurement)


def subscribe_orderbook(ticker,measurement):
    write_data(ticker,measurement)
    while True:
        sleep(10)
        write_data(ticker,measurement)

def create_process(ticker,measurement):
    process = multiprocessing.Process(target=subscribe_orderbook, args=(ticker,measurement))
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
    all_ticker = get_tickers()
    processes = []
    for ticker in all_ticker:
        processes.append(create_process(ticker,measurement))
        sleep(1)

    while (True):
        for entry in processes:
            process = entry["process"]
            ticker = entry["ticker"]
            if not process.is_alive():
                entry["process"].join()
                processes.remove(entry)
                processes.append(create_process(ticker,measurement))
        sleep(10)

        







