import random
import traceback
from time import sleep
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

def random_number_generator():
    rand = random.random()*1000 + random.random()*1000 + random.random()*1000 + random.random()*1000
    return rand

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



measurement = "huobidm_trades"


def trades_per_symbol(symbol_api,symbol_tag):
    trades = huobi.market_trade_batch(symbol_api,50)
    trades_data = trades['data']
    all_trades_data = []
    for td in trades_data:
        if len(td['data']) == 1:
            timestamp = [td['ts']+random_number_generator()]
        else:
            timestamp = [td['ts']+i+random_number_generator() for i in range(len(td['data']))]
        for idx,ts in enumerate(timestamp):
            fields = {}
            fields.update({'amount': float(td['data'][idx]['amount'])})
            fields.update({'direction':td['data'][idx]['direction']})
            fields.update({'price':float(td['data'][idx]['price'])})
            fields.update({'id':td['data'][idx]['id']})
            fields.update({'timestamp':timestamp[idx]})
            fields.update({'symbol':symbol_tag})
            all_trades_data.append(fields)
    return all_trades_data


def bulk_insert(measurement_name):
    all_trades = []
    all_symbols = get_valid_symbol()
    for symbv in all_symbols:
        print(trades_per_symbol(symbv,all_symbols[symbv]))
        all_trades += trades_per_symbol(symbv,all_symbols[symbv])
    print(len(all_trades))
    line_protocol_data = []
    for d in all_trades:
        line_protocol_data.append(
        '''
        {measurement},symbol={symbol} amount={amount},direction="{direction}",price={price},is_api_return_timestamp={tf},id={id}i {timestamp}
        '''
        .format(measurement=measurement_name,
                symbol=d['symbol'],
                amount=d['amount'],
                direction=d['direction'],
                price=d['price'],
                tf=True,
                id=d['id'],
                timestamp=int(d['timestamp'])))
    db.write_multiple_points_to_measurement(line_protocol_data,'ms')


def subscribe_trades_bulk_insert(measurement):
    bulk_insert(measurement)
    while True:
        sleep(60)
        try:
            bulk_insert(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)


def trades_update(symbol_api,measurement,symbol_tags):
    trades = huobi.market_trade_batch(symbol_api,2000)
    trades_data = trades['data']
    for td in trades_data:
        fields = {}
        if len(td['data']) == 1:
            timestamp = [td['ts']]
        else:
            timestamp = [td['ts']+i for i in range(len(td['data']))]
        for idx,ts in enumerate(timestamp):
            fields.update({'amount': float(td['data'][idx]['amount'])})
            fields.update({'direction':td['data'][idx]['direction']})
            fields.update({'price':float(td['data'][idx]['price'])})
            fields.update({'id':td['data'][idx]['id']})
            fields.update({"is_api_return_timestamp": True})
            tags = {}
            tags.update({"symbol":symbol_tags})
            dbtime = datetime.datetime.utcfromtimestamp(td['ts']/1000)
            db.write_points_to_measurement(measurement,dbtime,tags,fields)


def subscribe_trade(measurement):
    symbol_valid = get_valid_symbol()
    for symbv in symbol_valid:
        sleep(1)
        trades_update(symbv,measurement,symbol_valid[symbv])
    while True:
        sleep(60*5)
        symbol_valid = get_valid_symbol()
        for symbv in symbol_valid:
            sleep(1)
            try:
                trades_update(symbv, measurement, symbol_valid[symbv])
            except Exception:
                error_message = traceback.format_exc()
                #logger(measurement, error_message, symbv)

if __name__ == "__main__":
    subscribe_trade(measurement)
