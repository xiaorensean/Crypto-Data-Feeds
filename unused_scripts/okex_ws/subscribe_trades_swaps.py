import traceback
import multiprocessing as mp
import zlib
import websocket
import json
import time
import copy


# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
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

def reconnect(ws):
    time.sleep(1)


url = 'wss://real.okex.com:8443/ws/v3'




def trades_ws(ticker,measurement):
    #for channel in channels:
    ws = websocket.create_connection(url)
    channel = {"url": "swap/trade:{}".format(ticker),}
    sub_param = {"op": "subscribe", "args": channel["url"]}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)
    print(f"send: {sub_str}")

	    #add to sockets so don't have to reconnect
    channel["socket"] = ws

	    #initial response; check for error
    msg = ws.recv()
    msg_string = inflate(msg)
    response = json.loads(msg_string)
    if (response["event"] != "subscribe"):
	    print ("Connection to server failed!")
	    reconnect(ws)

    # sceond rsponse will receive data table
    msg = ws.recv()
    msg_string = inflate(msg)
    response = json.loads(msg_string)  
    data = response["data"]
    for d in data:
        time = None
        tags = {}
        tags.update({"symbol":d["instrument_id"]})
        fields = copy.copy(d)     
        del fields['instrument_id']
        influxdb.write_points_to_measurement(measurement, time, tags, fields)


def subscribe_trades(ticker,freq):
    measurement = 'okex_recentTrades'
    try:
        trades_ws(ticker,measurement)
    except Exception as err:
        error_message = str(err)
        #send_error_message(measurement,error_message)
        time.sleep(1)
    while True:
        time.sleep(freq)
        try:
            trades_ws(ticker,measurement)
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,ticker)
            time.sleep(1)


def trades_all_tickers(ticker):
    if "BTC" in ticker.split("-")[0] or "ETH" in ticker.split("-")[0]:
        # for all BTC ETH related, set freq to be 1 second
        subscribe_trades(ticker,1)
    else:
        # anything else set as 30 seconds
        subscribe_trades(ticker,20)


if __name__ == '__main__': 
    swap_data = get_swap_tickers("USD") + get_swap_tickers("USDT")
    symbols = [sd['contract'] for sd in swap_data]
    # all processes
    processes = {}
    for symb in symbols:
        orderbook = mp.Process(target=trades_all_tickers,args=(symb,))
        orderbook.start()
        processes.update({symb:orderbook})



