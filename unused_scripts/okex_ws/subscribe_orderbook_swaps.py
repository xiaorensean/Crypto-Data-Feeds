import multiprocessing as mp
import traceback
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
from subscribe_oi_swaps import get_swap_tickers
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger


influxdb = InfluxClient()


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
        


def orderbook_ws(ticker,measurement):
    url = 'wss://real.okex.com:8443/ws/v3'
    ws = websocket.create_connection(url)
    channel = {"url": "swap/depth:{}".format(ticker),}
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


def subscribe_orderbook_swap(ticker):
    if "BTC" in ticker or "ETH" in ticker:
        # for all BTC ETH related, set freq to be 1 second
        get_orderbook_data(ticker,1)
    else:
        # anything else set as 30 seconds
        get_orderbook_data(ticker,20)
        


 
if __name__ == '__main__':
    swap_tickers = [i['contract'] for i in get_swap_tickers("USD") + get_swap_tickers("USDT")]
    processes = {}
    for symb in swap_tickers:
        orderbook = mp.Process(target=subscribe_orderbook_swap,args=(symb,))
        orderbook.start()
        processes.update({symb:orderbook})


