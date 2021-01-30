import os
import sys
import time
from time import sleep
import ccxt 
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
api_key1 = '-gaNAWTlvPDqkVkjE-vLiGnE'#os.environ["BITMEX1_KEY"]
api_secret1 = 'aSkS1tRLXVycbr8JKlbzW25yL6TzJuBCLy6xdwpVVTg5Qchx'#os.environ["BITMEX1_SECRET"]

#api_key2 = os.environ["BITMEX3_KEY"]
#api_secret2 = os.environ["BITMEX3_SECRET"]

bitmex1 = ccxt.bitmex({"apiKey":api_key1,
                      "secret":api_secret1,
                      "timeout":30000})

#bitmex2 = ccxt.bitmex({"apiKey":api_key2,
#                      "secret":api_secret2,
#                      "timeout":30000})
def get_epochtime_ms():
    return int(time.time() * 1000.0)    

#bitmex = ccxt.bitmex({
#    'apiKey': 'API_KEY',
#    'secret': 'SECRET',
#    'timeout': 30000,
    # 'enableRateLimit': True,
#})
    
param_btc = {"symbol":"XBT",
         "depth":5}
param_eth = {"symbol":"ETH",
             "depth":5}
#orderbook = bitmex.publicGetOrderBookL2(param)
count = 0    
#orderbook = bitmex.publicGetOrderBookL2(param) 
#print(count)
while True:
    try:    
        time.sleep(1)
        count += 1
        print(count)
        fetching_timestamp = get_epochtime_ms()
        orderbook = bitmex1.publicGetOrderBookL2(param_btc)
        #orderbook = bitmex2.publicGetOrderBookL2(param_eth)
        fetched_timestamp = get_epochtime_ms()
        #server_timestamp = btc_usd_ticker['timestamp']    

        #print('server_timestamp  ', server_timestamp)
        print('fetching_timestamp', fetching_timestamp)
        print('fetched_timestamp ', fetched_timestamp)
        print('################################')    

        sleep_time = (1000 + fetching_timestamp - get_epochtime_ms()) / 1000
        if sleep_time > 0:
            sleep(sleep_time)
    except Exception as e:
        print('Got Exception :| ', e)
        sleep(2)
