from time import sleep
import traceback
import datetime 
import os
import sys
 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))
import huobi_api.HbRest as huobi

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger
from influxdb_client.influxdb_writer import Writer

db = Writer()

def all_symbols():
    contract_info = huobi.contract_info()
    symbols = list(set([d['symbol'] for d in contract_info['data']]))
    return symbols

def get_valid_symbol():
    contract_info = huobi.contract_info()
    symbols = {}
    for d in contract_info['data']:
        if d["contract_type"] == "this_week": 
            symbols.update({d['symbol']+"_CW":d['contract_code']})
        elif d["contract_type"] == "next_week":
            symbols.update({d['symbol']+"_NW":d['contract_code']})
        else:
            symbols.update({d['symbol']+"_CQ":d['contract_code']})
    return symbols 

# delivery price/ 1 minute
def delivery_price_update(symbol,measurement):
    delivery_price = huobi.contract_delivery_price(symbol)
    delivery_price_data = delivery_price['data']
    fields = {}
    fields.update({"delivery_price":float(delivery_price_data['delivery_price'])})
    fields.update({"is_api_return_timestamp": True})
    tags = {}
    tags.update({"symbol":symbol})
    dbtime = datetime.datetime.utcfromtimestamp(delivery_price['ts']/1000)
    db.write_points_to_measurement(measurement,dbtime,tags,fields)
    
# contracct risk info 
def contract_risk_info_update(symbol,measurement):
    risk_info = huobi.contract_risk_info(symbol)
    risk_info_data = risk_info['data']
    fields = {}
    fields.update({'insurance_fund':risk_info_data[0]['insurance_fund']}) 
    fields.update({'estimated_clawback':risk_info_data[0]['estimated_clawback']})
    fields.update({"is_api_return_timestamp": True})
    tags = {}
    tags.update({"symbol":symbol})
    dbtime = datetime.datetime.utcfromtimestamp(risk_info['ts']/1000) 
    db.write_points_to_measurement(measurement,dbtime,tags,fields)

def market_overview_update(symbol,measurement):
    symbol_clean = get_valid_symbol()
    for symbc in symbol_clean:
        market_overview = huobi.market_overview(symbc)
        market_overview_data = market_overview['tick']
        market_overview_data_clean = {d: market_overview_data[d] for d in market_overview_data if d != "bid" and d != 'ask'}
        fields = {}
        for d in market_overview_data_clean:
            try:
                fields.update({d:float(market_overview_data[d])})
            except:
                fields.update({d:market_overview_data[d]})
        fields.update({'ask_amt': float(market_overview_data['ask'][1])})
        fields.update({'ask_price': float(market_overview_data['ask'][0])})
        fields.update({'bid_amt': float(market_overview_data['bid'][1])})
        fields.update({'bid_price': float(market_overview_data['bid'][0])})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"contract_type":symbol_clean[symbc]})
        dbtime = datetime.datetime.utcfromtimestamp(market_overview_data['ts']/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)
# price limit
def contract_price_limit_update(symbol,measurement):
    contract_type = ["this_week","next_week","quarter"]
    for ct in contract_type:
        price_limit = huobi.contract_price_limit(symbol,ct)
        price_limit_data = price_limit['data'][0]
        fields = {i:price_limit_data[i] for i in price_limit_data if i != 'contract_code'}
        fields.update({"contract_type":ct})
        fields.update({"is_api_return_timestamp": True})
        tags = {}
        tags.update({"contract_code":price_limit_data["contract_code"]})
        dbtime = datetime.datetime.utcfromtimestamp(price_limit['ts']/1000)
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def subscribe_data():
    symbols = all_symbols()
    for symbol in symbols:
        measurement = "huobidm_contract_delivery_price"
        try:
            delivery_price_update(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol)
        sleep(1)
        measurement = "huobidm_contract_risk_info"
        try:
            contract_risk_info_update(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol)
        sleep(1)
        measurement = "huobidm_contract_market_overview" 
        try:
            market_overview_update(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol)
        sleep(1)
        measurement = "huobidm_contract_price_limit"
        try:
            contract_price_limit_update(symbol,measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message,symbol)



if __name__ == "__main__":
    subscribe_data()        
    while True:
        sleep(60)
        subscribe_data()

    







