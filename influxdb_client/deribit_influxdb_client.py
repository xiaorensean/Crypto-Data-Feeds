'''
Creates an instance of the InfluxDB client to write data to
Cleans and formats data to conform to database schema
Specific to Okex API
'''

import copy
import datetime as dt
import datetime
import random
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from influxdb_writer import Writer

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.emailer import send_email as send

TRADE_HISTORY = 250

class DeribitInfluxClient:

	def __init__(self):
		self.exchange = "deribit"
		self.writer = Writer()

		self.functions = {
			'ticker': self.ticker,
			'perpetual': self.perpetual,
			'trades': self.trades,
		}

		self.options_functions = {
			'ticker': self.options_ticker,
		}

	'''
	Helper functions to deal with different endpoints
	'''

	#this function takes in the endpoint and calls on a helper function
	def write(self, endpoint, symbol, data):
		return self.functions[endpoint](symbol, data)

	def ticker(self, symbol, data):
		measurement = "{}_ticker".format(self.exchange)
		time = dt.datetime.utcfromtimestamp(data['timestamp']/1000)
		tags = {"symbol": symbol}
		#fill in tags, deleting from data

		#all other data should be fields
		fields = copy.copy(data)
		fields.update({"is_api_return_timestamp": True})
		del fields['timestamp']
		del fields['instrument_name']
		for item in fields['stats'].keys():
			fields.update({item: fields['stats'][item]})
		del fields['stats']

		return self.write_to_measurement(measurement, time, tags, fields)
		pass

	def perpetual(self, symbol, data):
		measurement = "{}_fundingRate".format(self.exchange)
		#let influxDB set time
		time = False
		tags = {"symbol": symbol}
		#fill in tags, deleting from data

		#all other data should be fields
		fields = {"fundingRate": data['interest']}
		fields.update({"is_api_return_timestamp": False})
		return self.write_to_measurement(measurement, time, tags, fields)
		pass

	def trades(self, symbol, data):
		measurement = "{}_trades".format(self.exchange)

		for order in data:
			dbtime = datetime.datetime.utcfromtimestamp((order['timestamp'] + random.random())/1000)
			side = order['direction']
			tags = {}
			#fill in tags
			tags.update({"symbol": order['instrument_name']})
			#all other data should be fields, so delete all tags/timestamp
			#also delete funding-related data
			fields = copy.copy(order)
			fields.update({"is_api_return_timestamp": True})
			del fields['instrument_name']
			del fields['direction']
			del fields['timestamp']

			success = self.write_to_measurement(measurement, dbtime, tags, fields)
			if not success:
				return False

	'''
	Helper functions for options endpoints
	'''

	def options_write(self, endpoint, symbol, data):
		return self.options_functions[endpoint](symbol, data)

	def options_ticker(self, symbol, data):
		measurement = "{}_optionsTicker".format(self.exchange)
		time = dt.datetime.utcfromtimestamp(data['timestamp']/1000)
		tags = {"symbol": symbol}
		#fill in tags, deleting from data

		#all other data should be fields
		fields = copy.copy(data)
		fields.update({"is_api_return_timestamp": True})
		del fields['timestamp']
		del fields['instrument_name']
		for item in fields['stats'].keys():
			fields.update({item: fields['stats'][item]})
		del fields['stats']
		for item in fields['greeks'].keys():
			fields.update({item: fields['greeks'][item]})
		del fields['greeks']

		return self.write_to_measurement(measurement, time, tags, fields)
		pass


	'''
	Functions that interact directly with db
	'''

	def write_to_measurement(self, measurement, time, tags, fields):
		return self.writer.write_points_to_measurement(measurement, time, tags, fields)

	'''
	Error handling
	'''
	
	#send email to handle error
	def send_email(self, message):
		send(message)