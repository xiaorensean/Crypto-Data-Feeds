import traceback
import requests
import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

# utils
db = Writer()

def get_historical_borrow_supply(token_address):
    current_time = int(time.time())-60
    token_url = "https://api.compound.finance/api/v2/market_history/graph?asset={}&min_block_timestamp=1587325994&max_block_timestamp={}&num_buckets=64&network=mainnet".format(token_address,current_time)
    response = requests.get(token_url)
    data = response.json()
    dates = [d['block_timestamp'] for d in data['total_borrows_history']]
    total_borrow_history = [d['total']['value'] for d in data['total_borrows_history']]
    total_supply_history = [d['total']['value'] for d in data['total_supply_history']]
    exchange_rate = [d['rate'] for d in data['exchange_rates']]
    prices_usd =[d['price']['value'] for d in data['prices_usd']]

    #total_supply = []
    #for idx in range(len(total_supply_history)):
    #    ts = {}
    #    ts.update({"total_supply":float(total_supply_history[idx])*float(prices_usd[idx])*float(exchange_rate[idx])})
    #    ts.update({"timestamp":dates[idx]})
    #    total_supply.append(ts)

    #total_borrow = []
    #for idx in range(len(total_borrow_history)):
    #    tb = {}
    #    tb.update({"total_borrow":float(total_borrow_history[idx])*float(prices_usd[idx])})
    #    tb.update({"timestamp":dates[idx]})
    #    total_borrow.append(tb)
    total_supply = float(total_supply_history[-1])*float(prices_usd[-1])*float(exchange_rate[-1])
    total_borrow = float(total_borrow_history[-1])*float(prices_usd[-1])
    return total_supply,total_borrow

def get_token_info():
    marketoverview = "https://api.compound.finance/api/v2/ctoken?meta=true&network=mainnet"
    response = requests.get(marketoverview)
    data = response.json()
    metadata = data['meta']
    token_symb = []
    for d in data['cToken']:
        token_symb.append({d["symbol"]:d["token_address"]})
    return token_symb,metadata

def subscribe_compound_market():
    symbols = get_token_info()[0]
    metadata = get_token_info()[1]
    total_supply = 0
    total_borrow = 0
    for symb in symbols:
        symbol = [k for k in symb][0]
        address = [symb[k] for k in symb][0]
        total_supply += get_historical_borrow_supply(address)[0]
        total_borrow += get_historical_borrow_supply(address)[1]
    measurement = "compound_market_overview"
    fields = {}
    fields.update({"unique_borrowers":int(metadata["unique_borrowers"])})
    fields.update({"unique_suppliers":int(metadata["unique_suppliers"])})
    fields.update({"total_supply":float(total_supply)})
    fields.update({"total_borrow":float(total_borrow)})
    fields.update({"is_api_return_timestamp": False})
    print(fields)
    tags = {}
    dbtime = False
    db.write_points_to_measurement(measurement,dbtime,tags,fields)


if __name__ == '__main__':
    try:
        subscribe_compound_market()
    except Exception:
        error = traceback.format_exc()
        #logger("compound_market_overview",error)
    while (True):
        time.sleep(55)
        try:
            subscribe_compound_market()
        except Exception:
            pass
            #logger("compound_market_overview",error)

