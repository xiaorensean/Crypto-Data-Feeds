import traceback
import multiprocessing as mp
import zlib
import websocket
import json
import time
import datetime
import copy


# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
from subscribe_orderbook_futures_usd1 import get_future_tickers
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
    channel = {"url": "futures/trade:{}".format(ticker),}
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
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,ticker)
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

def get_trades_data_tickers():
    year_shortcut = str(datetime.datetime.now())[:2]
    future_tickers_usd = [i[:-4] + "-USD-" + year_shortcut + i[-4:] for i in get_future_tickers()[0]]
    future_tickers_usdt =[i[:-4] + "-" + year_shortcut + i[-4:] for i in get_future_tickers()[1]]
    future_tickers = future_tickers_usd + future_tickers_usdt
    process = {}
    for ftu in future_tickers:
        orderbook = mp.Process(target=trades_all_tickers,args=(ftu,))
        orderbook.start()
        process.update({ftu:orderbook})
    return process    


def subscribe_trades_future():
    processes = get_trades_data_tickers()
    while True:
        time.sleep(60*60*24)
        # Check for each time the tickers are rolling on 
        # Shut down all existing processes before starting new around
        for symbol in processes:
            processes[symbol].join()
        processes = get_trades_data_tickers()



if __name__ == '__main__':
    subscribe_trades_future()



