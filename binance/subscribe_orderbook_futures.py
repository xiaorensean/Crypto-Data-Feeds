import traceback
import multiprocessing as mp
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
import binance_api.BinanceRestApi as bapi
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utility
db = Writer()

def get_futures_symbol():
    futures_symbol = bapi.get_futures_tickers()
    symbols_future = [ii['symbol'] for ii in futures_symbol]#symbols_future = ["BTCUSDT","ETHUSDT"]
    return symbols_future

measurement = "binance_orderbook_futures"

def write_bid_ask(side,side_type,update_id,current_snapshot,measurement,symb):
    fields = {}
    for data in side:
        fields.update({"price":float(data[0])})
        fields.update({"qty":float(data[1])})
        fields.update({"lastUpdateID":update_id})
        fields.update({"ref_ts":int(current_snapshot)})
        fields.update({"type":side_type})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"symbol":symb})
        db_time = False
        db.write_points_to_measurement(measurement,db_time,tags,fields)
        
        
def write_data(symb,measurement,market_type):
    orderbook = bapi.get_orderbook(symb,market_type)
    try: 
        code = orderbook['code']
        return 
    except KeyError:
        pass
    update_id = orderbook["lastUpdateId"]
    current_snapshot = time.time()*1000
    ask = orderbook['asks']
    bid = orderbook['bids']
    write_bid_ask(ask,"ask",update_id,current_snapshot,measurement,symb)
    write_bid_ask(bid,"bid",update_id,current_snapshot,measurement,symb)


def subscribe_orderbook(symbols,measurement):
    try:
        write_data(symbols,measurement,"f")
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message,symbols)
    while True:
        # set up scheduler for 30 second
        time.sleep(30)
        try:
            write_data(symbols,measurement,"f")
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbols)


if __name__ == "__main__":
    # all processes
    processes = {}
    symbols_future = get_futures_symbol()
    for symb in symbols_future:
        orderbook = mp.Process(target=subscribe_orderbook,args=(symb,measurement))
        orderbook.start()
        processes.update({symb:orderbook})
