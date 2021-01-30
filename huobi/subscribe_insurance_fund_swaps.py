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


measurement = "huobidm_insurance_fund"



def all_symbols():
    contract_info = huobi.contract_info()
    symbols = list(set([d['symbol'] for d in contract_info['data']]))
    return symbols




# contract insurance fund 
def subscribe_insurance_fund(measurement):
    symbols = all_symbols()
    for symb in symbols:
        insurance_fund = huobi.get_swap_insurance_fund(symb)
        insurance_fund_data = insurance_fund['data']['tick']
        fields = {}
        for ifd in insurance_fund_data:
            fields.update({'insurance_fund':ifd['insurance_fund']})
            fields.update({"is_api_return_timestamp": True})
            tags = {}
            tags.update({'symbol':symb})
            dbtime = datetime.datetime.utcfromtimestamp(ifd['ts']/1000)
            db.write_points_to_measurement(measurement,dbtime,tags,fields)
            


if __name__ == "__main__":
    sleep(60*15)
    subscribe_insurance_fund(measurement)
    while True:
        sleep(60*60*24)
        try:
            subscribe_insurance_fund(measurement)
        except Exception:
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
        
