import asyncio
import nest_asyncio
import datetime
import requests
import zlib
import websockets
import json
import time
import copy
import datetime
import smtplib

# import scripts
import sys 
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from models.order_book import OrderBook

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.influxdb_client_v1 import InfluxClient

nest_asyncio.apply()
influxdb = InfluxClient()


def get_tickers(data):
    symbol_data = []
    for d in data:
        symbol_data += d['contracts']
    symbol = [i['name'] for i in symbol_data]  
    return symbol

# function to automatical fetch all tickers
def get_future_tickers():
    base = "https://www.okex.com/v3/"
    future_tickers_endpoint = "futures/pc/market/futuresCoin?currencyCode=0"
    future_tickers = base + future_tickers_endpoint
    response = requests.get(future_tickers)
    resp = response.json()
    data = resp['data']
    symbol_usdt = get_tickers(data['usdt'])
    symbol_usd = get_tickers(data['usd'])
    return symbol_usd, symbol_usdt



def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

#TODO
def reconnect(ws):
	time.sleep(1)

# send eamil 
def send_error_message(error_message):
    current_time = datetime.datetime.utcnow()
    subject = 'OKEX_orderbook'
    body = 'okex_orderbook script breaks at time {}\n'.format(current_time) + 'Error message is ' + error_message 
    message = 'Subject: {}\n\n{}'.format(subject,body)
    server = smtplib.SMTP_SSL('smtp.gmail.com',465)
    server.login("xiao@virgilqr.com","921211Rx")
    server.sendmail(msg = message, from_addr="xiao@virgilqr.com", to_addrs=["xiao@virgilqr.com"])
    server.quit()


def write_to_influxdb(order_book_type,measurement,data):
    fields = {}
    for entry in data[order_book_type]:
        time = None
        tags = {}
        tags.update({'symbol':data['instrument_id']})
        fields['price_depth'] = entry[0]
        fields['price_amount']  = entry[1]
        fields['orders_depth'] = entry[2]
        fields['type'] = order_book_type
        fields['timestamp'] = data['timestamp']
        influxdb.write_points_to_measurement(measurement, time, tags, fields)
        


async def orderbook_ws_connect_aio(channel):
    url = 'wss://real.okex.com:8443/ws/v3'
    async with websockets.connect(url) as ws: 
        sub_param = {"op": "subscribe", "args": channel["url"]}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)

        print(f"send: {sub_str}")
	    #initial response; check for error
        msg = await ws.recv()
        msg_string = inflate(msg)
        response = json.loads(msg_string)

        if (response["event"] != "subscribe"):
	        print ("Connection to server failed!")
	        reconnect(ws)
    
	    #second response is the initial snapshot of funding book
        msg = await ws.recv()
        msg_string = inflate(msg)
        response = json.loads(msg_string)
        data = response["data"][0]
        print(data)
	
    # initialise and add order book to channel
    order_book = OrderBook()
    order_book.update_from_snapshot(data)
    channel["order_book"] = order_book
    time.sleep(1)
    measurement = 'okex_Orderbook'
    # write bids to database
    write_to_influxdb('bids',measurement,data)
    # write aks to database 
    write_to_influxdb('asks',measurement,data)


async def get_orderbook_data(ticker):
    channel = {"url": "futures/depth:{}".format(ticker),}
    try:
        await orderbook_ws_connect_aio(channel)
    except Exception as err:
        error_message = str(err)
        print(error_message)
        #send_error_message(error_message)
        #time.sleep(1)


def subscribe_orderbook_future_usd():
    loop = asyncio.get_event_loop()
    year_shortcut = str(datetime.datetime.now())[:2]
    future_tickers_usd = [i[:-4] + "-USD-" + year_shortcut + i[-4:] for i in get_future_tickers()[0]]    
    get_data = [get_orderbook_data(t) for t in future_tickers_usd] 
    loop.run_until_complete(asyncio.gather(*get_data))



def subscribe_orderbook_future_usdt():
    loop = asyncio.get_event_loop()
    year_shortcut = str(datetime.datetime.now())[:2]
    future_tickers_usdt =[i[:-4] + "-" + year_shortcut + i[-4:] for i in get_future_tickers()[1]]   
    get_data = [get_orderbook_data(t) for t in future_tickers_usdt] 
    loop.run_until_complete(asyncio.gather(*get_data))





if __name__ == '__main__':
    a = time.perf_counter()
    subscribe_orderbook_future_usd()
    a1 = time.perf_counter()
    print(a1-a)


