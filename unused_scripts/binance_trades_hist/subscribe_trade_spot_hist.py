from datetime import datetime 
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from binance_api.client import Client
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.notification import send_error_message

# utility
db = Writer()

# get all tickers
#exchange_info = bapi.get_exchange_info()
#symbols = exchange_info['symbols']
symbols_spot = ["DGDBTC","DGDETH"]
#for symb in symbols:
#    if symb['status'] == "TRADING":
#        symbols_spot.append(symb['symbol'])
#    else:
#        pass
api_key = '08Xf4R01pGSfvUaLmxfbVqI8P6t8v1PqrhNUz5jCqSg4uhJ9PUlUFSJLupRKSdxd'
api_secret = '9BB4tiaV3GE21CUtfWEaba3yG0tFH28WThIcHz1KZSYDeh17PMDoBEmawX474MgS'
binance = Client(api_key,api_secret)

def write_to_table(symb):
    param = {"symbol":symb}
    last_trades = binance.get_historical_trades(param) 
    new_id = last_trades[-1]['id']
    start_ids = list(range(0,new_id,500))
    for i in start_ids:
        param_hist = {"symbol":symb,"fromId":i,"limit":500}
        time.sleep(0.1)
        try:
            trades = binance.get_historical_trades(param_hist) 
            subscribe_trade(trades,symb)
        except: 
            pass
        
def subscribe_trade(trade_data,symb):
    measurement = "binance_trade_spot"
    fields = {}
    for td in trade_data:
        fields.update({"id":td['id']})
        fields.update({"isBuyerMaker":td["isBuyerMaker"]})
        fields.update({"price":float(td["price"])})
        fields.update({"qty":float(td["qty"])})
        fields.update({"quoteQty":float(td["quoteQty"])})
        fields.update({"trade_timestamp":td["time"]})
        tags = {}
        tags.update({"symbol":symb})
        db_time = False
        db.write_points_to_measurement(measurement,db_time,tags,fields)

         
if __name__ == "__main__":
    for symb in symbols_spot:
        print('write ',symb)
        write_to_table(symb)

