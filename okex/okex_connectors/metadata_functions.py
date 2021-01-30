'''
Intemediary functions which connect websocket data to Influxdb client
'''

import websocket
import json
import requests
import dateutil.parser as dp
import zlib
import datetime as dt
import pandas as pd
import sys
from pandas.tseries.offsets import BMonthEnd, Week, Day, BQuarterEnd
from termcolor import colored
from threading import Timer

'''
OKex functions
'''

def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""

def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


'''
Futures-specific functions
'''

#gets next expiry date of futures
def get_future_suffixes():
    current_date = dp.parse(get_server_time())
    if current_date.hour < 8 and (0 < current_date.hour or 0 < current_date.minute): # 00:01 TO 07:59
        rounded_date = current_date.replace(microsecond=0,second=0,minute=0,hour=0)
    else:
        rounded_date = (current_date + pd.Timedelta("1 day")).replace(microsecond=0,second=0,minute=0,hour=0)

    times = []
    suffixes = []
    
    times.append(Week(weekday=4).rollforward(rounded_date) + pd.Timedelta("8 hr"))
    times.append(Week(weekday=4).rollforward(rounded_date + pd.Timedelta('7 days')) + pd.Timedelta('8 hr'))
    next_quarter = pd.to_datetime(BQuarterEnd().rollforward(rounded_date + pd.Timedelta("1 day"))) + pd.Timedelta("8 hr")
    #round next quarter to the past Friday
    dow = next_quarter.date().weekday()
    gap = (dow - 4)%7
    next_quarter = next_quarter - dt.timedelta(days = gap)
    times.append(next_quarter)
    global next_reset_time 
    next_reset_time = times[0]

    for time in times:
        word = str(time)
        suffix = word[2:4] + word[5:7] + word[8:10]
        suffixes.append(suffix)

    return suffixes

def get_next_expiry_time():
    current_date = dp.parse(get_server_time())
    if current_date.hour < 8 and (0 < current_date.hour or 0 < current_date.minute): # 00:01 TO 07:59
        rounded_date = current_date.replace(microsecond=0,second=0,minute=0,hour=0)
    else:
        rounded_date = (current_date + pd.Timedelta("1 day")).replace(microsecond=0,second=0,minute=0,hour=0)

    return Week(weekday=4).rollforward(rounded_date) + pd.Timedelta("8 hr") + pd.Timedelta("10 seconds")


'''
Main functions
'''

def get_all_suffixes():
    suffixes = get_future_suffixes()
    suffixes.append("SWAP")
    return suffixes

def get_swap_endpoints(pairs, endpoints, suffixes):
    channels = []
    mapping = {
        0 : "weekly",
        1 : "biweekly",
        2 : "quarterly",
        3 : None
    }
    for pair in pairs:
        for i in range(len(suffixes)):
            expiration = mapping[i]
            suffix = suffixes[i]
            types = []
            if expiration == None:
                types.append("swap/funding_rate:" + pair + suffix)
            for endpoint in endpoints:
                if expiration == None:
                    endpoint = "swap" + endpoint
                else:
                    endpoint = "futures" + endpoint
                name = pair + suffix
                types.append(endpoint + ":" + name)
            channels.append({"endpoints" : types, "symbol" : name, "expiration" : expiration})
    return channels

def subscribe(client, channel, endpoint, url = 'wss://real.okex.com:8443/ws/v3'):
    ws = websocket.create_connection(url)
    sub_param = {"op": "subscribe", "args": endpoint}
    sub_str = json.dumps(sub_param)
    ws.send(sub_str)

    while (True):
        # try:
        t = Timer(5, timeout_ping, args=[ws,])
        t.start()
        res = ws.recv()
        t.cancel()
        res = inflate(res)
        response = None
        try:
            response = json.loads(res)
        except:
            continue
        try:
            response_type = response["table"]
        except:
            #Initial response to confirm subscription
            try:
                response_type = response["event"]
                continue
            #Some kind of error
            except:
                print(colored("Invalid response from server", "red"))
                ws.close()
                sys.exit(0)

        symbol = response["data"][0]["instrument_id"]
        endpoint = response_type.split("/")[1]
        data = response["data"]
        client.write(endpoint, symbol, data, channel["expiration"])
        #Write to db, check for errors
        #if not client.write(endpoint, symbol, data, channel["expiration"]):
        #    print(colored("Database error!", "yellow"))
        #    client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "okex/subscribe_swap_data.py"))
        #    sys.exit(1)

        # except:
        #     ws.close()
        #     client.send_email("Subject: {}\n\n{}".format("ERROR with Script", "okex/subscribe_swap_data.py"))
        #     reconnect(url, client)

def timeout(endpoint, symbol):
    if endpoint == "ticker" or endpoint == "trade":
        if (symbol == "BTC-USD-SWAP" or symbol == "ETH-USD-SWAP" or symbol == "LTC-USD-SWAP"):
            print(colored(endpoint + symbol, "red"))
            sys.exit(2)

def timeout_ping(ws):
    ws.send("ping")


