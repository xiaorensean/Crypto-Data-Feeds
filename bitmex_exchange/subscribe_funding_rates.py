import requests
from time import sleep
import traceback
import datetime
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.bitmex_influxdb_client_v1 import BitmexInfluxClient
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger



# subscribe realtime data 
bitmex = BitmexInfluxClient()
influxdb = InfluxClient()


#symbols = ["XBTUSD","ETHUSD","XRPUSD"]
measurement = 'bitmex_indicative_funding_rates'


def funding_rate_history():
    funding_url = 'https://www.bitmex.com/api/v1/funding?reverse=True'
    funding_rates = requests.get(funding_url)
    funding_rates_data = funding_rates.json()
    return funding_rates_data



def funding_rates_update(measurement):
    symbols = list(set([frh['symbol'] for frh in funding_rate_history()]))
    funding_rates = []
    for symb in symbols:
        funding_rates = bitmex.get_bitmex_funding_rates(symb)
        funding_rates += funding_rates
    for fr in funding_rates:
        bitmex.bitmex_funding_rates_update(fr,measurement)
    

def subscribe_funding_rates(measurement):
    funding_rates_update(measurement)
    while True:
        sleep(60)
        try:
            funding_rates_update(measurement)
        except Exception:
            error_time = datetime.utcnow()
            error_message = traceback.format_exc()
            pass
            #logger(measurement,error_message)
            # write to log file
            #with open("error_log.txt", "a") as text_file:
            #    print("Error message for bitmex_funding_rates_{} at {} \n".format(error_time) + error_message, file=text_file)
            #pass
        
 
def check_data_per_symbol(symbol):
    funding_rates = funding_rate_history()
    funding_rates_symbol = [fr for fr in funding_rates if fr['symbol'] == symbol]
    db_data = influxdb.query_tables("bitmex_funding_rates",["distinct(fundingRate)","where symbol = '{}' and time > '{}' and time < '{}'".\
                                                        format(symbol,funding_rates_symbol[1]['timestamp'],funding_rates_symbol[0]['timestamp'])])
    db_data = db_data['distinct'].tolist()
    funding_rates_data = funding_rates_symbol[0]
    if funding_rates_data['fundingRate'] in db_data:
        print(symbol,"Match")
    else:
        #send_error_message("BitMEX Funding Rate Not Match","Bitmex Funding Hist is not match with instrument.")
        print(symbol,"Not Match")


def check_data():
    symbols = list(set([frh['symbol'] for frh in funding_rate_history()]))
    for symb in symbols:
        check_data_per_symbol(symb)
    while True:
        sleep(60*60*8)
        for symb in symbols:
            check_data_per_symbol(symb)

    
if __name__ == '__main__':
    subscribe_funding_rates(measurement)


    
        
