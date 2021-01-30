import traceback
import time
import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
from namebase_api.namebaseDomainApi import *
sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_writer import Writer
from utility.error_logger_writer import logger

db = Writer()

domains_popular = []

# functions to write data
def write_data(data,measurement,domain_type):
    for d in data:
        fields = {}
        fields.update({"name":d["name"]})
        fields.update({"reveal_block":d["reveal_block"]})
        fields.update({"total_number_bids":d["total_number_bids"]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"domain_type":domain_type})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_data_anticipated(data,measurement):
    for d in data:
        fields = {}
        fields.update({"name":d["name"]})
        fields.update({"release_block":d["release_block"]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"domain_type":"bid_soon"})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

def write_data_recently_sold(data,measurement):
    for d in data:
        fields = {}
        fields.update({"name":d["name"]})
        fields.update({"close_amount":d["close_amount"]})
        fields.update({"total_number_bids":d["total_number_bids"]})
        fields.update({"is_api_return_timestamp": False})
        tags = {}
        tags.update({"domain_type":"recently_sold"})
        dbtime = False
        db.write_points_to_measurement(measurement,dbtime,tags,fields)

       
# popular domain
def domain_popular_collection(measurement):
    for i in range(0,1000000,12):
        dp = domain_popular(i)['domains']
        if len(dp) != 0:
            write_data(dp,measurement,"bid_now")
        else:
            break

# ending soon domain
def domain_ending_soon_collection(measurement):
    for i in range(0,1000000,12):
        des = domain_ending_soon(i)['domains']
        if len(des) != 0:
            write_data(des,measurement,"ending_soon")
        else:
            break

# anticipated domain
def domain_anticipated_collection(measurement):
    for i in range(0,1000000,12):
        da = domain_anticipated(i)['domains']
        if len(da) != 0:
            write_data_anticipated(da,measurement)
        else:
            break

def domain_reconetly_sold_collection(measurement):
    for i in range(0,1000000,12):
        drs = domain_recently_sold(i)['domains']
        if len(drs) != 0:
            write_data_recently_sold(drs,measurement)
        else:
            break 
        
def subscribe_domain():
    measurement = "namebase_domain"  
    print("domain_popular")
    try:
        domain_popular_collection(measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)
    print("domain_ending_soon")
    try:
        domain_ending_soon_collection(measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)  
    print("domain_anticipated")
    try:
        domain_anticipated_collection(measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)
    print("domain_recently_sold")
    try:
        domain_reconetly_sold_collection(measurement)
    except Exception:
        error_message = traceback.format_exc()
        logger(measurement,error_message)


if __name__ == "__main__":
    subscribe_domain()
    while True:
        time.sleep(60*60*9)
        subscribe_domain()
