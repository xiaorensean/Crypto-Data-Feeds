import traceback
from multiprocessing import Process
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from ftx_api.FtxWs import FtxWebsocketClient
from ftx_api.FtxRest import get_contract_names

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

ftx_ws = FtxWebsocketClient()
db = Writer()

all_markets = get_contract_names()

measurement = "ftx_orderbook"

def btc_eth_tickers():
    all_tickers = get_contract_names()
    tickers = [i for i in [i for i in all_tickers if "BTC" in i or "ETH" in i] if "MOVE" not in i]
    return tickers



# write bid ask data to influxdb 
def write_bid_ask(ob_type,bid_ask,measurement,symbol,current_time):
    fields = {}
    for b in bid_ask:
        fields.update({"snapshot":current_time})
        fields.update({"price":b[0]})
        fields.update({"size":b[1]})
        fields.update({"type":ob_type})
        tags = {}
        tags.update({"symbol":symbol})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)   

# subscribe orderbook data
def write_metadata(orderbook_data,market,measurement):
    timestamp = str(orderbook_data['timestamp'])
    bids = orderbook_data['bids']
    asks = orderbook_data['asks']
    write_bid_ask("ask",asks,measurement,market,timestamp)
    write_bid_ask("bid",bids,measurement,market,timestamp)


def subscribe_orderbook(symbol,measurement):
    btc_eth_ticker = btc_eth_tickers()
    if symbol in btc_eth_ticker:
        while True:
            try:
                data = ftx_ws.get_orderbook(symbol)
            except:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbol)
            try:
                write_metadata(data,symbol,measurement)
            except:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbol)
    else:
        while True:
            time.sleep(10)
            try:
                data = ftx_ws.get_orderbook(symbol)
            except:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbol)
            try:
                write_metadata(data,symbol,measurement)
            except:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbol)


if __name__ == "__main__":
    all_markets = get_contract_names()
    for market in all_markets:
        freq_subscription = Process(target=subscribe_orderbook,args=(market,measurement))
        freq_subscription.start()
 
    
