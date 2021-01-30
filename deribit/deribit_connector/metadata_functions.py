'''
Intemediary functions which connect websocket data to Influxdb client
'''

import websocket
import json
import sys
from termcolor import colored

def get_swap_endpoints():
	channels = [
		#"ticker.BTC-PERPETUAL.raw",
		#"ticker.ETH-PERPETUAL.raw",
		"perpetual.BTC-PERPETUAL.raw",
		"perpetual.ETH-PERPETUAL.raw",
	]
	return channels

def get_swap_ticker():
	tickers = [
		"BTC-PERPETUAL",
		"ETH-PERPETUAL"
	]
	return tickers

'''
Getting tickers
'''

def get_instruments(tickers, category):
	instruments = []
	ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
	for ticker in tickers:
		msg = \
		{
			"jsonrpc" : "2.0",
			"id" : 7617,
			"method" : "public/get_instruments",
			"params" : {
				"currency" : ticker,
				"kind" : category,
				"expired" : False
			}
		}
		ws.send(json.dumps(msg))
		response = json.loads(ws.recv())
		for item in response["result"]:
			if item['instrument_name'].split("-")[1] != "PERPETUAL":
				instruments.append(item['instrument_name'])
	ws.close
	return instruments

def get_futures_endpoints(tickers, endpoints):
	channels = []
	instruments = get_instruments(tickers, "future")
	for endpoint in endpoints:
		for instrument in instruments:
			channels.append("{}.{}.raw".format(endpoint, instrument))
	return channels

def get_futures_ticker(tickers, endpoints):
	instruments = get_instruments(tickers, "future")
	return instruments

def get_options_endpoints(tickers, endpoints):
	channels = []
	instruments = get_instruments(tickers, "option")
	for endpoint in endpoints:
		for instrument in instruments:
			channels.append("{}.{}.raw".format(endpoint, instrument))
	return channels


'''
API/Database methods
'''


def call_futures_api(client, msg):
  ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
  ws.send(msg)
  while (True):
    response = ws.recv()
    params = None
    try:
      params = json.loads(response)['params']
    except:
      continue
    endpoint = params['channel'].split('.')[0]
    symbol = params['channel'].split('.')[1]
    data = params['data']
    #write to db
    if not client.write(endpoint, symbol, data):
      print(colored("Database error!", "yellow"))
      client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "deribit/subscribe_futures_data.py"))
      sys.exit(1)

def call_options_api(client, msg):
  ws = websocket.create_connection('wss://www.deribit.com/ws/api/v2')
  ws.send(msg)
  while (True):
    response = ws.recv()
    params = None
    try:
      params = json.loads(response)['params']
    except:
      continue
    endpoint = params['channel'].split('.')[0]
    symbol = params['channel'].split('.')[1]
    data = params['data']
    #write to db
    if not client.options_write(endpoint, symbol, data):
      print(colored("Database error!", "yellow"))
      client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "deribit/subscribe_options_data.py"))
      sys.exit(1)
