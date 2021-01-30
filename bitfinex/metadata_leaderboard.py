import requests
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)

from influxdb_client.influxdb_client_v1 import InfluxClient

db = InfluxClient()


# all rank measure
all_rank_measure = {"plu_diff":["1w:tGLOBAL:USD","1M:tGLOBAL:USD"],
                "plu":["1w:tGLOBAL:USD","3h:tGLOBAL:USD","3h:tBTCUSD",
                       "3h:tBTCF0:USTF0","3h:tBTCUST","3h:tBTCEUR",
                       "3h:tLEOUSD","3h:tETHUSD","3h:tETHF0:USTF0",
                       "3h:tETHUST","3h:tEOSUSD","3h:tLTCUSD",
                       "3h:tETCUSD"],
                "vol":["1w:tGLOBAL:USD","1M:tGLOBAL:USD","3h:tGLOBAL:USD",
                       "3h:tBTCUSD","3h:tBTCF0:USTF0","3h:tBTCUST",
                       "3h:tETHUSD","3h:tETHF0:USTF0","3h:tETHUST",
                       "3h:tEOSUSD","3h:tLTCUSD","3h:tETCUSD"],
                "plr":["1M:tGLOBAL:USD"]}



def get_bitfinex_leaderboard(rank_measure,rank_type,rank_period):
    leaderboard_endpoint = "https://api-pub.bitfinex.com/v2/rankings/{}:{}/hist?limit={}".format(rank_measure,rank_type,rank_period)
    response = requests.get(leaderboard_endpoint)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print ("Error: " + str(response.status_code) + str(response.json()))
        return
    
# get all rank data
def get_rank_data(rank_measure,rank_period):
    all_data = {}
    for rank_m in rank_measure.keys():
        for rank_type in rank_measure[rank_m]:
            data_temp = get_bitfinex_leaderboard(rank_m,rank_type,rank_period)
            data = [i for i in data_temp]
            all_data.update({rank_m+"_"+rank_type:data})
    return all_data

# write all data to influxdb
def write_data(measurement,data,data_type):
    for dt in data:
        symbol = dt.split("_")[-1]
        for d in data[dt]:
            fields = {}
            fields.update({"ref_ts":int(d[0])})
            fields.update({"name":d[2]})
            fields.update({"rank":int(d[3])})
            fields.update({"amount":float(d[6])})
            fields.update({"is_api_return_timestamp": True})
            tags = {}
            tags.update({"symbol":symbol})
            tags.update({"type":data_type})
            dbtime = False
            db.write_points_to_measurement(measurement, dbtime, tags, fields)  
            



# unrealized profit delta
def write_unrealized_profit_period_delta(measurement,param):
    unrealized_profit_pd = get_rank_data(param,10000)             
    data_type = "unrealized_profit_period_delta"
    write_data(measurement,unrealized_profit_pd,data_type)
    
# unrealized profit inception
def write_unrealized_profit_inception(measurement,param):
    unrealized_profit_i = get_rank_data(param,10000)             
    data_type = "unrealized_profit_inception"
    write_data(measurement,unrealized_profit_i,data_type)

# volume
def write_volume(measurement,param):
    vol = get_rank_data(param,10000)            
    data_type = "volume"
    write_data(measurement,vol,data_type)

# unrealized profit inception
def write_realized_profit(measurement,param):
    realized_profit_data = get_rank_data(param,10000)            
    data_type = "realized_profit"
    write_data(measurement,realized_profit_data,data_type)

# monthly
def write_monthly_data(measurement):
    month_unrealized_profit_period_delta = {"plu_diff":["1M:tGLOBAL:USD"]}
    write_unrealized_profit_period_delta(measurement,month_unrealized_profit_period_delta)
    month_volume = {"vol":["1M:tGLOBAL:USD","1M:tETHF0:USTF0","1M:tBTCF0:USTF0"]}
    write_volume(measurement,month_volume)
    month_realized_profit = {"plr":["1M:tGLOBAL:USD"]}
    write_realized_profit(measurement,month_realized_profit)
    
# weekly 
def write_weekly_data(measurement):
    week_unrealized_profit_period_delta = {"plu_diff":["1w:tGLOBAL:USD"]}
    write_unrealized_profit_period_delta(measurement,week_unrealized_profit_period_delta)
    week_unrealized_profit_inception = {"plu":["1w:tGLOBAL:USD"]}
    write_unrealized_profit_inception(measurement,week_unrealized_profit_inception)
    week_volume = {"vol":["1w:tGLOBAL:USD","1w:tGLOBAL:CHZ"]}
    write_volume(measurement,week_volume)
    
# three-hourly
def write_hourly_data(measurement):
    hour_unrealized_profit_inception = {"plu":["3h:tGLOBAL:USD","3h:tBTCUSD",
                       "3h:tBTCF0:USTF0","3h:tBTCUST","3h:tBTCEUR",
                       "3h:tLEOUSD","3h:tETHUSD","3h:tETHF0:USTF0",
                       "3h:tETHUST","3h:tEOSUSD","3h:tLTCUSD",
                       "3h:tETCUSD"]}
    write_unrealized_profit_inception(measurement,hour_unrealized_profit_inception)
    hour_volume = {"vol":["3h:tGLOBAL:USD",
                       "3h:tBTCUSD","3h:tBTCF0:USTF0","3h:tBTCUST",
                       "3h:tETHUSD","3h:tETHF0:USTF0","3h:tETHUST",
                       "3h:tEOSUSD","3h:tLTCUSD","3h:tETCUSD"]}
    write_volume(measurement,hour_volume)


