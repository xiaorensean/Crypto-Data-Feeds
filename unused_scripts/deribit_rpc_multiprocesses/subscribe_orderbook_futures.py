'''
Subscribe to orderbook for all BTC-based swaps, futures, and options
'''
import traceback
import multiprocessing
#import nest_asyncio
import time

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata
from deribit_api.deribitRestApi import RestClient
sys.path.append(os.path.dirname( current_dir ))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger
deribit_api = RestClient()
db = Writer()



def get_swap_tickers():
    
    swap_tickers = metadata.get_swap_ticker()
    
    return swap_tickers

def get_futures_tickers():
    tickers = [
      "BTC", 
      "ETH"
              ]
    endpoints = [
      "ticker",
              ]
    future_tickers = metadata.get_futures_ticker(tickers, endpoints)
    return future_tickers

def get_options_tickers():
    option_tickers_raw = metadata.get_options_endpoints(['BTC','ETH'],['ticker'])
    option_tickers = [i.split(".")[1] for i in option_tickers_raw]
    return option_tickers
    
measurement = "deribit_orderbook"


def write_ask_bid(data,side_type,measurement):
    fields = {}
    side_data = data[side_type]
    timestamp = data['tstamp']
    instrument_name = data['instrument']
    for sd in side_data:
        fields.update({"price":float(sd['price'])})
        fields.update({"amount":float(sd['amount'])})
        fields.update({"timestamp":timestamp})
        fields.update({"type":side_type})
        tags = {"symbol":instrument_name}
        time = False
        db.write_points_to_measurement(measurement,time,tags,fields)


def subscribe_orderbook(ticker,measurement):
    try:
        data = deribit_api.getorderbook(ticker,200)
        print(data)
    except Exception:
        error = traceback.format_exc()
        logger(measurement, error,ticker)
        data = None
        pass
    if data is not None:
        print("Writting ", ticker)
        write_metadata(data)
    else:
        print(ticker," not avaiable")
    while True:
        time.sleep(20)
        try:
            data = deribit_api.getorderbook(ticker,200)
            print(data)
        except Exception:
            error = traceback.format_exc()
            logger(measurement,error,ticker)
            data = None
            pass
        if data is not None:
            print("Writting ", ticker)
            write_metadata(data)
        else:
            print(ticker," not avaiable")
    
        
def write_metadata(data):
    # subscribe option orderbook data
    measurement = "deribit_orderbook"
    write_ask_bid(data,"asks",measurement)
    write_ask_bid(data,"bids",measurement)

        
def get_rolling_futures_tickers():
    futures_tickers = get_futures_tickers()
    processes = {}
    for symb in futures_tickers:
        print(symb)
        process = multiprocessing.Process(target=subscribe_orderbook, args=(symb,measurement))
        process.start()
        processes.update({symb: process})
    return processes


def subscribe_orderbook_rolling_tickers():
    processes = get_rolling_futures_tickers()
    while True:
        time.sleep(60*60*24*30)
        # Check for each time the tickers are rolling on 
        # Shut down all existing processes before starting new around
        for symbol in processes:
            processes[symbol].join()
        processes = get_rolling_futures_tickers()

if __name__ == "__main__":
    subscribe_orderbook_rolling_tickers()

#swap_tickers = get_swap_tickers()
#futures_tickers = get_futures_tickers()
#option_tickers = get_option_tickers()

#tickers = swap_tickers + futures_tickers + option_tickers
#all_data = []
#for i in tickers:
#    print(i)
#    data = deribit_api.getorderbook(i,200)
#    all_data.append(data)







