import time
import datetime
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from namebase_api.namebaseApi import get_depth
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.notification import send_error_message

db = Writer()


def update_orderbook(symbol,data,side_type,ts,mesurement):
    last_event_id = data['lastEventId']
    side_data = data[side_type]
    fields = {}
    for s in side_data:
        fields.update({"current_timetamp":ts})
        fields.update({"price":float(s[0])})
        fields.update({"amount":float(s[1])})
        fields.update({"type":side_type})
        fields.update({"last_event_id":last_event_id})
        dbtime = False
        tags = {}
        tags.update({"symbol":symbol})
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

measurement = "namebase_orderbook"    
symbol = "HNSBTC"    

    
def subscribe_orderbook(symbol,measuremnt):
    start_ts = time.perf_counter()
    data = get_depth(symbol)
    ts = str(datetime.datetime.now())
    print(ts)
    update_orderbook(symbol,data,"asks",ts,measurement)
    update_orderbook(symbol,data,"bids",ts,measurement)
    end_ts = time.perf_counter()
    print("Time elpased: ", end_ts-start_ts)


if __name__ == "__main__":
    #try: 
    subscribe_orderbook(symbol,measurement)
    #except Exception as err:
    #    error_message = str(err)
    #    send_error_message(measurement,error_message)
    while True:
        time.sleep(30)
        #try: 
        subscribe_orderbook(symbol,measurement)
        #except Exception as err:
        #    error_message = str(err)
        #    send_error_message(measurement,error_message)