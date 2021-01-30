'''
Subscribe to orderbook for all BTC-based swaps, futures, and options
'''
import time
import datetime as dt
import multiprocessing
import copy
#import nest_asyncio
import time as times

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import deribit_connector.metadata_functions as metadata
from deribit_api.deribitWS import get_last_trades_by_currency, get_orderbook, get_index 
sys.path.append(os.path.dirname( current_dir ))
from influxdb_client.influxdb_writer import Writer

db = Writer()


# get option tickers based on recent trades
def get_option_tickers_based_on_recent_trades():
    symbols = ["BTC","ETH"]
    trades = []
    for symb in symbols:
        trades += get_last_trades_by_currency(symb,500)
    # filter tickers based on recent trades
    option_tickers = [at for at in list(set([t['instrument_name'] for t in trades])) if at[-2:] == "-C" or at[-2:] == "-P"]
    return option_tickers


# get option tickers based on index price and strike price
def get_option_tickers_based_on_atm(symbol,diff_threshould):
    index_price = get_index(symbol)[symbol]
    option_ep = metadata.get_options_endpoints([symbol],['ticker'])
    option_tickers_all = sorted([symb.split(".")[1] for symb in option_ep])
    option_tickers_symbol = [aot for aot in option_tickers_all if symbol in aot]
    strike_price_all = list(sorted(set([int(ots.split("-")[-2]) for ots in option_tickers_symbol])))
    index_strike_diff = {spa:index_price - spa for spa in strike_price_all}
    closest_price = []
    for isd in index_strike_diff:
        if abs(index_strike_diff.get(isd)) < diff_threshould:
            closest_price.append(isd)
        else:
            pass

    closest_price_ticker = []
    for ots in option_tickers_symbol:
        for cp in closest_price:
            if int(ots.split("-")[-2]) == cp:
                closest_price_ticker.append(ots)
    return closest_price_ticker
            

def get_option_tickers_based_on_atm_expir():
    all_tickers = metadata.get_options_endpoints(['BTC','ETH'],['ticker'])
    # filter for month
    option_tickers_expir = [i for i in all_tickers if "MAR" in i or i in "APR"] 
    # filter for strike price
    strike_price_filter = [str(ii) for ii in list(set([int(i.split("-")[2]) for i in option_tickers_expir])) if ii >= 6000 and ii <=10000]
    option_tickers = []
    for top in option_tickers_expir:
        for spf in strike_price_filter:
            if spf in top:
                option_tickers.append(top)
            else:
                pass
    return option_tickers




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


def options_ticker(data):
    measurement = "deribit_optionsTicker"
    time = dt.datetime.utcfromtimestamp(data['timestamp']/1000)
    tags = {"symbol": data['instrument_name']}
    fields = copy.copy(data)
    del fields['asks']
    del fields['bids']
    del fields['timestamp']
    del fields['instrument_name']
    for item in fields['stats'].keys():
        fields.update({item: fields['stats'][item]})
    del fields['stats']
    for item in fields['greeks'].keys():
        fields.update({item: fields['greeks'][item]})
    del fields['greeks']
    db.write_points_to_measurement(measurement, time, tags, fields)
    
    
def subscribe_orderbook(ticker):
    try:
        data = get_orderbook(ticker,200)
        print(data)
    except Exception as err:
        print (err)
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
            data = get_orderbook(ticker,200)
            print(data)
        except Exception as err:
            print (err)
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
    # subscribe option ticker data
    options_ticker(data)
        


def get_rolling_tickers():
    option_tickers_raw = metadata.get_options_endpoints(['ETH'],['ticker'])
    option_tickers = [i.split(".")[1] for i in option_tickers_raw]
    processes = {}
    for symb in option_tickers:
        time.sleep(0.1)
        print(symb)
        process = multiprocessing.Process(target=subscribe_orderbook, args=(symb,))
        process.start()
        processes.update({symb: process})
    return processes


def subscribe_orderbook_rolling_tickers():
    processes = get_rolling_tickers()
    while True:
        time.sleep(60)
        # Check for each time the tickers are rolling on 
        # Shut down all existing processes before starting new around
        for symbol in processes:
            processes[symbol].join()
        processes = get_rolling_tickers()


if __name__ == "__main__":
    subscribe_orderbook_rolling_tickers()








