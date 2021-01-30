'''
Intemediary functions which connect websocket data to Influxdb client
'''

import bitmex
import websocket
import json
from termcolor import colored
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__))))
#from bitmex_api.Bxapi import bitmex

'''
Subscribe to a particular endpoint for metadata
'''

client = None
orderbook = None
x = None

def subscribe(client, suffix = 'instrument,funding,insurance,settlement,liquidation,trade'):
	connect_string = 'wss://www.bitmex.com/realtime?subscribe=' + suffix
	ws = websocket.create_connection(connect_string)

	while (True):
		res = ws.recv()
		response = None
		try:
			response = json.loads(res)
		except:
			continue

		endpoint = None
		data = None
		action = None
		try:
			endpoint = response["table"]
			data = response["data"]
			action = response["action"]
		except:
			try:
				if not response["success"]:
					print(colored("ERROR", "red"))
				continue
			except:
				continue

		if not (client.write(endpoint, action, data)):
			print(colored("Database error!", "yellow"))
			client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "bitmex/subscribe_data.py"))
			sys.exit(1)

def subscribe_slow(passed_client, suffix = 'instrument,funding,insurance,settlement,liquidation,trade'):
	global client
	client = passed_client
	connect_string = 'wss://www.bitmex.com/realtime?subscribe=' + suffix
	ws = websocket.WebSocketApp(connect_string,
		on_message = on_message_subscribe_slow,
        on_error = on_error_subscribe_slow,
        on_close = on_close_subscribe_slow)
	ws.run_forever(ping_interval=60, ping_timeout=10)
	
def on_message_subscribe_slow(ws, message):
	response = None
	try:
		response = json.loads(message)
	except:
		return

	endpoint = None
	data = None
	action = None
	try:
		endpoint = response["table"]
		data = response["data"]
		action = response["action"]
	except:
		try:
			if not response["success"]:
				print(colored("ERROR", "red"))
			return
		except:
			return

	if not (client.write(endpoint, action, data)):
		print(colored("Database error!", "yellow"))
		client.send_email("Subject: {}\n\n{}".format("ERROR with Influxdb", "bitmex/subscribe_data.py"))
		sys.exit(1)

def on_error_subscribe_slow(ws, error):
	sys.exit(1)

def on_close_subscribe_slow(ws):
    sys.exit(1)



'''
Susbcribe to an orderbook
'''


def on_message_orderbook(ws, message):
	try:
		response = json.loads(message)
		action = response["action"]
		orderbook.process(action, response)
	except:
		try:
			success = json.loads(message)["success"]
			if not success:
				#exit and restart script
				sys.exit(1)
		except:
			return

def on_error_orderbook(ws, error):
	print(colored(x,"red"))
	# orderbook.client.send_email("Subject: {}\n\n{}".format("ERROR with Bitmex orderbook", error))
	sys.exit(1)

def on_close_orderbook(ws):
    print(colored(x,"red"))
    # orderbook.client.send_email("Subject: {}\n\n{}".format("ERROR with Bitmex orderbook", "websocket closed"))
    # Attemp to reconnect with 2 seconds interval
    sys.exit(1)

'''
Get symbols for all XBT and ETH instruments
'''

def get_symbols():
	channels = []
	client = bitmex.bitmex(test = False)

	response = client.Instrument.Instrument_getActive().result()

	if str(response[1]) != "200 OK":
		sys.exit(0)

	for entry in response[0]:
		channels.append({"symbol" : entry["symbol"]})
	return(channels)

if __name__ == "__main__":
    a = [symb["symbol"] for symb in get_symbols()]
    print(a)












