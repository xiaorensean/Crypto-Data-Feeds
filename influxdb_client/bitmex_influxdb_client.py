'''
Creates an instance of the InfluxDB client to write data to
Cleans and formats data to conform to database schema
Specific to Bitmex API
'''

import copy
import time
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

class BitmexInfluxClient:

	def __init__(self):
		self.exchange = "bitmex"
		self.writer = Writer()

		self.functions = {
			'instrument': self.instrument,
			'trade': self.trade,
			'funding': self.funding,
			'liquidation': self.liquidation,
			'insurance': self.insurance,
			'settlement': self.settlement,
		}

	'''
	Helper functions to deal with different enpoints
	'''

	#this function takes in the endpoint and calls on a helper function
	def write(self, endpoint, action, data):
		return self.functions[endpoint](action, data)

	def instrument(self, action, data):
		for entry in data:
			#write to db
			measurement = "{}_instrument".format(self.exchange)
			tags = {}
			#fill in tags
			tags.update({"symbol": entry["symbol"]})
			#all other data should be fields, so delete all tags/timestamp
			#also delete funding-related data
			fields = copy.copy(entry)
			del fields['symbol']
			del fields['timestamp']
			fields.update({"is_api_return_timestamp": True})
			ts = " ".join(entry['timestamp'].split("Z")[0].split("T"))
			dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
			uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
			dt = datetime.datetime.utcfromtimestamp(uts / 1000)
			dbtime = dt
			#write everything as float
			for key, value in fields.items():
				if type(value) == int:
					fields[key] = float(value)

			success = self.write_to_measurement(measurement, dbtime, tags, fields)
			if not success:
				return False

		return True

	def funding(self, action, data):
		try:
			for entry in data:
				#write to db
				measurement = "{}_funding_rates".format(self.exchange)
				time = entry['timestamp']
				tags = {}
				#fill in tags
				tags.update({"symbol": entry["symbol"]})
				#all other data should be fields, so delete all tags/timestamp
				#also delete funding-related data
				fields = copy.copy(entry)
				del fields['timestamp']
				del fields['symbol']
				fields.update({"is_api_return_timestamp": True})
				success = self.write_to_measurement(measurement, time, tags, fields)
				if not success:
					return False
		except:
			#write to db
			measurement = "{}_funding".format(self.exchange)
			time = data['timestamp']
			tags = {}
			#fill in tags
			tags.update({"symbol": data["symbol"]})
			#all other data should be fields, so delete all tags/timestamp
			#also delete funding-related data
			fields = copy.copy(data)
			del fields['timestamp']
			del fields['symbol']
			fields.update({"is_api_return_timestamp": True})
			return self.write_to_measurement(measurement, time, tags, fields)
		return True

	def trade(self, action, data):
		print(data)
		try:
			for entry in data:
				print(entry)
				#write to db
				measurement = "{}_trades".format(self.exchange)
				ts = " ".join(entry['timestamp'].split("Z")[0].split("T"))
				dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
				uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
				dt = datetime.datetime.utcfromtimestamp(uts / 1000)
				dbtime = dt
				tags = {}
				#fill in tags
				tags.update({"symbol": entry["symbol"]})
				#all other data should be fields, so delete all tags/timestamp
				#also delete funding-related data
				fields = copy.copy(entry)
				del fields['symbol']
				del fields['timestamp']
				fields.update({"is_api_return_timestamp": True})
				#write everything as float
				for key, value in fields.items():
					if type(value) == int:
						fields[key] = float(value)

				success = self.write_to_measurement(measurement, dbtime, tags, fields)
				if not success:
					return False
		except:			#write to db
			measurement = "{}_trades".format(self.exchange)
			ts = " ".join(data['timestamp'].split("Z")[0].split("T"))
			dt_temp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
			uts = time.mktime(dt_temp.timetuple()) * 1000 + random.random()
			dt = datetime.datetime.utcfromtimestamp(uts / 1000)
			dbtime = dt
			tags = {}
			#fill in tags
			tags.update({"symbol": data["symbol"]})
			#all other data should be fields, so delete all tags/timestamp
			#also delete funding-related data
			fields = copy.copy(data)
			fields.update({"is_api_return_timestamp": True})
			del fields['symbol']
			del fields['timestamp']
			return self.write_to_measurement(measurement, dbtime, tags, fields)
		return True


	def liquidation(self, action, data):
		try:
			for entry in data:
				#write to db
				measurement = "{}_liquidation".format(self.exchange)
				time = False
				tags = {}
				#fill in tags
				tags.update({"symbol": entry["symbol"], "side": entry["side"]})
				#all other data should be fields, so delete all tags/timestamp
				#also delete funding-related data
				fields = copy.copy(entry)
				del fields['symbol']
				del fields['side']
				fields.update({"is_api_return_timestamp": False})
				#write everything as float
				for key, value in fields.items():
					if type(value) == int:
						fields[key] = float(value)

				success = self.write_to_measurement(measurement, time, tags, fields)
				if not success:
					return False
			return True
		except:
			return True

	def settlement(self, action, data):
		try:
			for entry in data:
				#write to db
				measurement = "{}_settlement".format(self.exchange)
				time = entry['timestamp']
				tags = {}
				#fill in tags
				tags.update({"symbol": entry["symbol"]})
				#all other data should be fields, so delete all tags/timestamp
				#also delete funding-related data
				fields = copy.copy(entry)
				del fields['timestamp']
				del fields['symbol']
				fields.update({"is_api_return_timestamp": True})
				#write everything as float
				for key, value in fields.items():
					if type(value) == int:
						fields[key] = float(value)

				success = self.write_to_measurement(measurement, time, tags, fields)
				if not success:
					return False
		except:
			#write to db
			measurement = "{}_settlement".format(self.exchange)
			time = data['timestamp']
			tags = {}
			#fill in tags
			tags.update({"symbol": data["symbol"]})
			#all other data should be fields, so delete all tags/timestamp
			#also delete funding-related data
			fields = copy.copy(data)
			del fields['timestamp']
			del fields['symbol']
			fields.update({"is_api_return_timestamp": True})
			return self.write_to_measurement(measurement, time, tags, fields)
		return True

	def insurance(self, action, data):
		for entry in data:
			measurement = "{}_insurance".format(self.exchange)
			time = entry["timestamp"]
			tags = {}
			fields = copy.copy(entry)
			del fields["timestamp"]
			fields.update({"is_api_return_timestamp": True})
			success = self.write_to_measurement(measurement, time, tags, fields)
			if not success:
				return False
		return True
	


	'''
	Cryptofeed functions
	'''

	def crypto_instrument(self):
		pass

	def cryptofeed_trade(self, timestamp, pair, side, id, price, amount):
		measurement = "{}_trade".format(self.exchange)
		time = None
		tags = {
			'symbol': pair,
			'side' : side,
		}
		fields = {
			'official_timestamp' : timestamp,
			'trdMatchID' : str(id),
			'price' : float(price),
			'size' : float(amount),
		}
		return self.write_to_measurement(measurement, time, tags, fields)
		



	'''
	Functions that interact directly with db
	'''

	def write_to_measurement(self, measurement, time, tags, fields):
		return self.writer.write_points_to_measurement(measurement, time, tags, fields)

	'''
	Error handling
	'''
	def send_email(self, message):
		send(message)


