from time import sleep
import traceback
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
measurement = "huobidm_open_interest"

contract_info = huobi.contract_info()
tickers = list(set([d['symbol'] for d in contract_info['data']]))

contract_type = ["this_week","next_week","quarter"]
freqs = ["4hour","12hour","1day"]

def volume_hist_update(symbol,contract_type,freq):
    # volume in cont volume
    oi_hist_1 = huobi.contract_open_interest_hist(symbol,contract_type,freq,200,1)
    oi_hist_data_1 = oi_hist_1['data']['tick']
    # volume in crypto amount
    oi_hist_2 = huobi.contract_open_interest_hist(symbol,contract_type,freq,200,2)
    oi_hist_data_2 = oi_hist_2['data']['tick']

    fields = {}
    for idx,item in enumerate(oi_hist_data_1):
        fields.update({"ts":item["ts"]})
        fields.update({"volume":float(item["volume"])})
        fields.update({"amount":float(oi_hist_data_2[idx]['volume'])})
        fields.update({"contract_type":contract_type})
        fields.update({"contract_code":None})
        tags = {}
        tags.update({"symbol":symbol})
        time = False
        db.write_points_to_measurement(measurement,time,tags,fields)
        

for symb in tickers:
    for ct in contract_type:
        for freq in freqs:
            try:
                volume_hist_update(symb,ct,freq)
            except Exception:
                tag = symb + ":" + freq
                error = traceback.format_exc()
                #logger(measurement, error,tag)
            sleep(0.1)
