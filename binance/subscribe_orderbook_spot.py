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

symbols_spot = ["DGDBTC","DGDETH","BCHUSDT","BCHBTC","BTCUSDT","ETHUSDT",\
                "KNCBTC","KNCETH","RENBTC","RENUSDT","RENBNB","BNBUSDT",\
                "BNBETH","BNBBTC","SOLBNB","SOLBTC","SOLBUSD","KNCUSDT",\
                "LINKUSDT", "LINKBTC", "LENDBTC","LENDUSDT", "LRCBTC",\
                "LRCUSDT","REPBTC","REPUSDT","KAVABTC","KAVAUSDT","ASTBTC",\
                "BNTBTC","BNTUSDT","BANDBTC","BANDUSDT","INJBTC","INJBNB",\
                "INJBUSD", "INJUSDT"]


measurement = "binance_orderbook_spot"


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
        
        
def write_orderbook_data(symb,measurement,market_type):
    orderbook = bapi.get_orderbook(symb,market_type)
    try: 
        code = orderbook['code']
        return 
    except KeyError:
        pass
    update_id = orderbook["lastUpdateId"]
    current_snapshot = time.time() * 1000
    ask = orderbook['asks']
    bid = orderbook['bids']
    write_bid_ask(ask,"ask",update_id,current_snapshot,measurement,symb)
    write_bid_ask(bid,"bid",update_id,current_snapshot,measurement,symb)


def subscribe_orderbook(symbols_spot,measurement):
    if symbols_spot == "INJBTC" or symbols_spot == "INJUSDT":
        try:
            write_orderbook_data(symbols_spot,measurement,"s")
        except Exception:
            error_message = traceback.format_exc()
            logger(measurement,error_message,symbols_spot)
        while True:
            # set up scheduler for 5 seconds
            time.sleep(5)
            try:
                write_orderbook_data(symbols_spot,measurement,"s")
            except Exception:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbols_spot)
    else:
        try:
            write_orderbook_data(symbols_spot,measurement,"s")
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbols_spot)
        while True:
            # set up scheduler for 30 seconds
            time.sleep(30)
            try:
                write_orderbook_data(symbols_spot,measurement,"s")
            except Exception:
                error_message = traceback.format_exc()
                #logger(measurement,error_message,symbols_spot)

if __name__ == "__main__":
    # all processes
    processes = {}
    for symb in symbols_spot:
        trades = mp.Process(target=subscribe_orderbook,args=(symb,measurement))
        trades.start()
        processes.update({symb:trades})





