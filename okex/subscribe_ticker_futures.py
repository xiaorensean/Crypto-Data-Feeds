import traceback
import datetime 
import time
import requests
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "okex_ticker_futures"

# function to automatical fet all tickers
def get_tickers():
    base = "https://www.okex.com/v2/"
    market_overview_endpoint = "futures/pc/market/marketOverview.do?symbol=f_usd_all"
    market_overview = base + market_overview_endpoint
    response = requests.get(market_overview)
    resp = response.json()
    data = resp['ticker']
    all_tickers = list(set([d['symbolName'] for d in data]))
    symbol_usdt = ["f_usdt_"+i for i in all_tickers]
    symbol_usd = ["f_usd_"+i for i in all_tickers]
    return [symbol_usdt,symbol_usd]


# fetch ticker info
def post_ticker_info(param):
    base = "https://www.okex.com/v2"
    oi_url = "/futures/pc/market/tickers.do"
    tickers = base + oi_url
    response = requests.post(tickers,data=param)
    resp = response.json()
    if resp["msg"] == "success":
        data = resp['data']
    return data


# collect usdt denomination
def data_collection_usd(symbols):
    usdt_symbol_data = []
    for symb in symbols:
        param = {"symbol": symb}
        try:
            data = post_ticker_info(param)
        except UnboundLocalError:
            data = []
        usdt_symbol_data += data
    usdt_symbol_clean = []
    # current_snapshot = str(datetime.datetime.now())
    for symb_data in usdt_symbol_data:
        fields = symb_data
        # add symbol
        symbol = symb_data['symbol']
        contractId = symb_data['contractId']
        symbol_temp = symbol.split("_")
        symbol_char = symbol_temp[-1] + "-" + symbol_temp[1]
        symbol_char = symbol_char.upper()
        symbol_new = symbol_char + "-" + str(contractId)[2:8]
        # add open interest
        convert_rate = symb_data['volume'] / symb_data['coinVolume']
        coin_oi = symb_data['holdAmount'] / convert_rate
        usd_oi = symb_data['holdAmount'] * symb_data["unitAmount"]
        fields.update({"coin_denominated_open_interest": float(coin_oi)})
        fields.update({"usd_denominated_open_interest": float(usd_oi)})
        fields.update({"contract_symbol": symbol_new})
        usdt_symbol_clean.append(fields)
    return usdt_symbol_clean


# collect usdt denomination
def data_collection_usdt(symbols):
    usdt_symbol_data = []
    for symb in symbols:
        param = {"symbol": symb}
        try:
            data = post_ticker_info(param)
        except UnboundLocalError:
            data = []
        usdt_symbol_data += data
    usdt_symbol_clean = []
    # current_snapshot = str(datetime.datetime.now())
    for symb_data in usdt_symbol_data:
        fields = symb_data
        # add symbol
        symbol = symb_data['symbol']
        contractId = symb_data['contractId']
        symbol_temp = symbol.split("_")
        symbol_char = symbol_temp[-1] + "-" + symbol_temp[1]
        symbol_char = symbol_char.upper()
        symbol_new = symbol_char + "-" + str(contractId)[2:8]
        # add open interest
        coin_oi = symb_data['holdAmount'] * float(symb_data['unitAmount'])
        usd_oi = coin_oi * float(symb_data["buy"])
        fields.update({"coin_denominated_open_interest": float(coin_oi)})
        fields.update({"usd_denominated_open_interest": float(usd_oi)})
        fields.update({"contract_symbol": symbol_new})
        usdt_symbol_clean.append(fields)
    return usdt_symbol_clean


def subscribe_open_interest(measurement):
    all_symbols_data = []
    all_tickers = get_tickers()
    symbol_usdt = all_tickers[0]
    symbol_usd = all_tickers[1]
    usdt_symbol_data = data_collection_usdt(symbol_usdt)
    usd_symbol_data = data_collection_usd(symbol_usd)
    all_symbols_data = all_symbols_data + usdt_symbol_data + usd_symbol_data
    for asd in all_symbols_data:
        fields = {i: asd[i] for i in asd if i != "contract_symbol" and i != "symbol"}
        fields.update({"is_api_return_timestamp": False})
        db_time = False
        tags = {}
        tags.update({"symbol":asd["contract_symbol"]})
        db.write_points_to_measurement(measurement,db_time,tags,fields)
    

if __name__ == '__main__':
    try:
        subscribe_open_interest(measurement)
    except Exception:
        error_message = traceback.format_exc()
        #logger(measurement,error_message)
    while True:
        time.sleep(1)
        try:
            subscribe_open_interest(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
