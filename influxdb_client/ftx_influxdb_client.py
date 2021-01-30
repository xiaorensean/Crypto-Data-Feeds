import datetime 
import copy
import sys
import os
import arrow
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from influxdb_client_v1 import InfluxClient
api_dir = os.path.dirname(current_dir) + "/ftx"
sys.path.append(api_dir)
from ftx_api.FtxRest import get_contract_names, get_all_tickers, get_trades, get_funding_rates, get_futures_stats

#import warnings
#from arrow.factory import ArrowParseWarning
#warnings.simplefilter("ignore",ArrowParseWarning)

class FTXInfluxDBClient:
    
    def __init__(self):
        self.influxdb = InfluxClient()
    
    
    # fetch historical trade data
    def write_historical_trade_data(self,symbol,measurement):
        start_time = 1545255585
        end_time = datetime.datetime.now().timestamp()
        print("Write " + symbol + " to influxdb.")
        trade = get_trades(symbol,start_time,end_time)
        if len(trade) == 0 :
            print(symbol, "No data")
            return
        else:
            for t in trade:
                data_entry = t 
                tags = {}
                tags = {"symbol":symbol}
                fields = data_entry
                time = data_entry['time']
                fields = copy.copy(data_entry)
                fields.update({"is_api_return_timestamp": True})
                del fields["time"]
                self.influxdb.write_points_to_measurement(measurement,time,tags,fields)
    
    
    # update trade data
    def write_recent_trade_data(self,symbol,measurement):
        print("Update " + symbol + " to influxdb.")
        ftx_trade = self.influxdb.query_tables('FTX_trades',["*","WHERE symbol='{}' ORDER BY time DESC LIMIT 1".format(symbol)])
        dt = ftx_trade.time.tolist()[0]    
        ts = arrow.get(dt)
        start_time = ts.timestamp
        end_time = datetime.datetime.now().timestamp()
        trade = get_trades(symbol,start_time,end_time)
        for t in trade[:-1]:
            data_entry = t 
            tags = {}
            tags = {"symbol":symbol}
            fields = data_entry
            time = data_entry['time']
            fields = copy.copy(data_entry)
            fields.update({"is_api_return_timestamp": True})
            del fields["time"]
            self.influxdb.write_points_to_measurement(measurement,time,tags,fields)
    
    # check for newly update tickers 
    def subscribe_trade_data(self):
        measurement = 'ftx_trades'
        symbol_current = get_all_tickers()
        try:
            symbol_db = self.influxdb.get_tag_values('FTX_trades','symbol')
        except IndexError:
            for symb in symbol_current:
                self.write_historical_trade_data(symb,measurement)
        # mannulty exclude empty item
        symbol_current_filter = [i for i in symbol_current if i != "DOGEHEDGE/USD" and i != "XTZHEDGE/USD" and i != "XAUTHEDGE/USD"]
        # new symbols added or symbols been replaced
        symbol_diff = [symb for symb in symbol_current_filter if symb not in symbol_db]
        #if len(symbol_diff) != 0:
        #    for symb in symbol_diff:
        #        print("Historical backfilled")
        #        self.write_historical_trade_data(symb,measurement)
        #else:
        for symb in symbol_current_filter:
            self.write_recent_trade_data(symb,measurement)
    
    
    # funding rates for FTX
    def subscribe_funding_rates(self):
        measurement = 'ftx_funding_rates'
        funding_rates = get_funding_rates()
        for fr in funding_rates:
            data_entry = fr
            tags = {}
            tags = {"symbol":data_entry['future']}
            fields = data_entry
            time = data_entry['time']
            fields = copy.copy(data_entry)
            fields.update({"is_api_return_timestamp": True})
            del fields['future']
            del fields['time']
            self.influxdb.write_points_to_measurement(measurement,time,tags,fields)

    
    # future stats
    def subscribe_future_stats(self):
        measurement = "ftx_future_stats"
        future_contracts = get_contract_names()
        for future_contract in future_contracts:
            data_entry = get_futures_stats(future_contract)
            tags = {}
            tags = {"symbol":future_contract}
            fields = data_entry
            fields.update({"is_api_return_timestamp": False})
            time = False
            self.influxdb.write_points_to_measurement(measurement,time,tags,fields)
    

if __name__ == '__main__':
    ftx = FTXInfluxDBClient()
    a = ftx.subscribe_trade_data()
    
    
    
    
    