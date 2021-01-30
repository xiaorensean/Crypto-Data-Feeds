import traceback
import multiprocessing as mp
import datetime
import requests
import zlib
import websocket

import json
import time


# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from models.order_book import OrderBook

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()



def get_tickers(data):
    symbol_data = []
    for d in data:
        symbol_data += d['contracts']
    symbol = [i['name'] for i in symbol_data]  
    return symbol

# function to automatical fetch all tickers
def get_future_tickers():
    base = "https://www.okex.com/v3/"
    future_tickers_endpoint = "futures/pc/market/futuresCoin?currencyCode=0"
    future_tickers = base + future_tickers_endpoint
    response = requests.get(future_tickers)
    resp = response.json()
    data = resp['data']
    symbol_usdt = get_tickers(data['usdt'])
    symbol_usd = get_tickers(data['usd'])
    return symbol_usd, symbol_usdt

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

#TODO
def reconnect(ws):
	time.sleep(1)


def write_to_influxdb(order_book_type,measurement,data):
    fields = {}
    for entry in data[order_book_type]:
        time = None
        tags = {}
        tags.update({'symbol':data['instrument_id']})
        fields['price_depth'] = entry[0]
        fields['price_amount']  = entry[1]
        fields['orders_depth'] = entry[2]
        fields['type'] = order_book_type
        fields['timestamp'] = data['timestamp']
        influxdb.write_points_to_measurement(measurement, time, tags, fields)
        

def orderbook_ws_on_connect(ticker,measurement):
    url = 'wss://real.okex.com:8443/ws/v3'
    ws = websocket.create_connection(url)
    channel = {"url": "futures/depth:{}".format(ticker),}
    sub_param = {"op": "subscribe", "args": channel["url"]}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)

    print(f"send: {sub_str}")
	#initial response; check for error
    msg = ws.recv()
    msg_string = inflate(msg)
    response = json.loads(msg_string)

    if (response["event"] != "subscribe"):
        print ("Connection to server failed!")
        reconnect(ws)
    while True:
	    #second response is the initial snapshot of funding book
        msg = ws.recv()
        msg_string = inflate(msg)
        response = json.loads(msg_string)
        data = response["data"][0]
        print(data)
	    # initialise and add order book to channel
        order_book = OrderBook()
        order_book.update_from_snapshot(data)
        channel["order_book"] = order_book
        time.sleep(1)
        measurement = 'okex_Orderbook'
        # write bids to database
        write_to_influxdb('bids',measurement,data)
        # write aks to database 
        write_to_influxdb('asks',measurement,data)


def orderbook_ws(ticker,measurement):
    url = 'wss://real.okex.com:8443/ws/v3'
    ws = websocket.create_connection(url)
    channel = {"url": "futures/depth:{}".format(ticker),}
    sub_param = {"op": "subscribe", "args": channel["url"]}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)

    print(f"send: {sub_str}")
	#initial response; check for error
    msg = ws.recv()
    msg_string = inflate(msg)
    response = json.loads(msg_string)

    if (response["event"] != "subscribe"):
        print ("Connection to server failed!")
        reconnect(ws)
    
    #second response is the initial snapshot of funding book
    msg = ws.recv()
    msg_string = inflate(msg)
    response = json.loads(msg_string)
    data = response["data"][0]
    print(data)
	# initialise and add order book to channel
    order_book = OrderBook()
    order_book.update_from_snapshot(data)
    channel["order_book"] = order_book
    time.sleep(1)
    # write bids to database
    write_to_influxdb('bids',measurement,data)
    # write aks to database 
    write_to_influxdb('asks',measurement,data)
    
    
def get_orderbook_data(ticker,freq):
    measurement = 'okex_Orderbook'
    try:
        orderbook_ws(ticker,measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,ticker)
        time.sleep(1)
        pass
    while True:
        time.sleep(freq)
        try:
            orderbook_ws(ticker,measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,ticker)
            time.sleep(1)
            pass


def orderbook_all_tickers(ticker):
    if "BTC" in ticker or "ETH" in ticker:
        # for all BTC ETH related, set freq to be 1 second
        get_orderbook_data(ticker,1)
    else:
        # anything else set as 30 seconds
        get_orderbook_data(ticker,20)
        


def get_orderbook_data_tickers():
    year_shortcut = str(datetime.datetime.now())[:2]
    future_tickers_usd = [i[:-4] + "-USD-" + year_shortcut + i[-4:] for i in get_future_tickers()[0]]
    future_tickers_usdt =[i[:-4] + "-" + year_shortcut + i[-4:] for i in get_future_tickers()[1]]
    future_tickers = future_tickers_usd + future_tickers_usdt
    process = {}
    for ftu in future_tickers:
        orderbook = mp.Process(target=orderbook_all_tickers,args=(ftu,))
        orderbook.start()
        process.update({ftu:orderbook})
    return process    
    
 
def subscribe_orderbook_future():
    processes = get_orderbook_data_tickers()
    while True:
        time.sleep(60*60)
        # Check for each time the tickers are rolling on 
        # Shut down all existing processes before starting new around
        for symbol in processes:
            processes[symbol].join()
        processes = get_orderbook_data_tickers()



if __name__ == '__main__':
    subscribe_orderbook_future()



