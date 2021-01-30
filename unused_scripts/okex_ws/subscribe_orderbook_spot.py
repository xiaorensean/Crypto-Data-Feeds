import traceback
import multiprocessing as mp
import websocket
import json
import zlib
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


symbols = ["HBAR-USDT","HBAR-BTC","HBAR-USDK","ALGO-USDT","ALGO-USDK",\
           "ALGO-BTC","ALGO-ETH","BTC-USDT","ETH-USDT"]


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

# send eamil 

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
    channel = {"url": "spot/depth:{}".format(ticker),}
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
	#initialise and add order book to channel
    order_book = OrderBook()
    order_book.update_from_snapshot(data)
    channel["order_book"] = order_book
    time.sleep(1)
    measurement = 'okex_Orderbook'
    # write bids to database
    write_to_influxdb('bids',measurement,data)
    # write aks to database 
    write_to_influxdb('asks',measurement,data)


def subscribe_orderbook(ticker,freq):
    measurement = 'okex_Orderbook'
    try:
        orderbook_ws(ticker,measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,ticker)
        print(error_message)
        time.sleep(1)
        pass
    while True:
        time.sleep(freq)
        try:
            orderbook_ws(ticker,measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,ticker)
            print(error_message)
            time.sleep(1)
            pass



def orderbook_all_tickers(ticker):
    if "BTC" in ticker.split("-")[0] or "ETH" in ticker.split("-")[0]:
        # for all BTC ETH related, set freq to be 1 second
        subscribe_orderbook(ticker,1)
    else:
        # anything else set as 30 seconds
        subscribe_orderbook(ticker,20)



if __name__ == '__main__':    
    # all processes
    processes = {}
    for symb in symbols:
        orderbook = mp.Process(target=orderbook_all_tickers,args=(symb,))
        orderbook.start()
        processes.update({symb:orderbook})
        
    #while True:
    #    for symb in processes:
    #        if not processes[symb].is_alive():
    #            orderbook = mp.Process(target=subscribe_orderbook,args=(symb,))
    #            orderbook.start()
    #            processes.update({symb:orderbook})
    #        else:
    #            pass

