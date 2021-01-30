import traceback
import time
import datetime
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from namebase_api.namebaseApi import get_depth
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()


def update_orderbook(symbol,data,side_type,ts,mesurement):
    last_event_id = data['lastEventId']
    side_data = data[side_type]
    fields = {}
    for s in side_data:
        fields.update({"ref_ts":int(ts)})
        fields.update({"price":float(s[0])})
        fields.update({"amount":float(s[1])})
        fields.update({"type":side_type})
        fields.update({"last_event_id":last_event_id})
        fields.update({"is_api_return_timestamp": False})
        dbtime = False
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

measurement = "namebase_orderbook"    
symbol = "HNSBTC"    

    
def subscribe_orderbook(symbol,measuremnt):
    data = get_depth(symbol)
    ts = time.time() * 1000
    update_orderbook(symbol,data,"asks",ts,measurement)
    update_orderbook(symbol,data,"bids",ts,measurement)


if __name__ == "__main__":
    try: 
        subscribe_orderbook(symbol,measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message,symbol)
    while True:
        time.sleep(10)
        try: 
            subscribe_orderbook(symbol,measurement)
        except Exception:
            time.sleep(2)
            try:
                subscribe_orderbook(symbol,measurement)
            except Exception:
                error_message = traceback.format_exc()
                logger(measurement,error_message,symbol)