'''
Creates an instance of the InfluxDB client to write data to
Cleans and formats data to conform to database schema
Specific to Okex API
'''

import copy
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from influxdb_client.influxdb_writer import Writer
from influxdb_client.influxdb_client_v1 import InfluxClient

pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.emailer import send_email as send

TRADE_HISTORY = 100

db = InfluxClient()

class OkexInfluxClient:

    def __init__(self, channels):
        self.exchange = "okex"
        self.writer = Writer()

        self.past_trades = {}
        self.last_funding_time = {}

        for channel in channels:
            for endpoint in channel["endpoints"]:
                self.past_trades.update({endpoint.split(":")[1]: []})

        self.functions = {
            'ticker': self.ticker,
            'price_range': self.price_range,
            'mark_price': self.mark_price,
            'trade': self.trade,
            'funding_rate': self.funding_rate,
        }

    '''
    Helper functions to deal with different enpoints
    '''

    # this function takes in the endpoint and calls on a helper function
    def write(self, endpoint, symbol, data, expiration=None):
        return self.functions[endpoint](symbol, data, expiration)

    def ticker(self, symbol, data, expiration):
        data = data[0]
        symbol = data['instrument_id']
        # write to db
        measurement = "{}_ticker".format(self.exchange)
        dbtime = data['timestamp']
        tags = {}
        # fill in tags
        tags.update({"symbol": symbol, "expiration": expiration})
        # all other data should be fields, so delete all tags/timestamp
        # also delete funding-related data
        fields = copy.copy(data)
        del fields['timestamp']
        del fields['instrument_id']
        fields_clean = {f:float(fields[f]) for f in fields}
        fields_clean.update({"is_api_return_timestamp": True})
        return self.write_to_measurement(measurement, dbtime, tags, fields_clean)

    def price_range(self, symbol, data, expiration):
        data = data[0]
        symbol = data['instrument_id']
        # write to db
        measurement = "{}_priceRange".format(self.exchange)
        dbtime = data['timestamp']
        tags = {}
        # fill in tags
        tags.update({"symbol": symbol, 'expiration': expiration})
        # all other data should be fields, so delete all tags/timestamp
        # also delete funding-related data
        fields = copy.copy(data)

        del fields['timestamp']
        del fields['instrument_id']
        fields_clean = {}
        fields_clean.update({"highest":float(fields["highest"])})
        fields_clean.update({"lowest":float(fields["lowest"])})
        fields_clean.update({"is_api_return_timestamp": True})

        return self.write_to_measurement(measurement, dbtime, tags, fields_clean)

    def mark_price(self, symbol, data, expiration):
        data = data[0]
        measurement = "okex_markPrice"
        symbol = data['instrument_id']
        print(symbol)
        try:
            data_db = db.query_tables(measurement, ["*", "where symbol = '{}' order by time desc limit 1".format(symbol)], "raw")[0][0]
        except:
            data_db = None
        if data_db is not None:
            if float(data['mark_price']) == float(data_db['mark_price']):
                print("Price is the same")
                pass
            else:
                dbtime = data['timestamp']
                tags = {}
                tags.update({"symbol": symbol, 'expiration': expiration})
                fields = {}
                fields.update({"mark_price":float(data['mark_price'])})
                fields.update({"is_api_return_timestamp": True})
                self.write_to_measurement(measurement, dbtime, tags, fields)
        else:
            dbtime = data['timestamp']
            tags = {}
            tags.update({"symbol": symbol, 'expiration': expiration})
            fields = {}
            fields.update({"mark_price": float(data['mark_price'])})
            fields.update({"is_api_return_timestamp": True})
            self.write_to_measurement(measurement, dbtime, tags, fields)

    def trade(self, symbol, data, expiration):
        new_orders = []
        prev_orders = self.past_trades[symbol]
        for order in data:
            if (order in prev_orders):
                continue
            else:
                new_orders.append(order)
                prev_orders.append(order)
                if (len(prev_orders) > TRADE_HISTORY):
                    prev_orders.pop(0)
        prev_orders.sort(key=lambda x: x["timestamp"])
        new_orders.sort(key=lambda x: x["timestamp"])

        for order in new_orders:
            # write to db
            measurement = "{}_trades".format(self.exchange)
            time = None
            tags = {
                "symbol": symbol,
                "expiration": expiration,
                "side": order["side"]
            }
            # all other data should be fields, so delete symbol and timestamp
            fields = copy.copy(order)
            del fields['instrument_id']
            del fields["side"]

            success = self.write_to_measurement(measurement, time, tags, fields)
            if not success:
                return False

        return True

    # For Okex, funding rate is continually updated
    def funding_rate(self, symbol, data, expiration):

        funding_time = data[0]["funding_time"]
        funding_rate = float(data[0]["funding_rate"])
        interest_rate = float(data[0]["interest_rate"])
        instrument_id = data[0]["instrument_id"]
        self.last_funding_time[symbol] = funding_time

        # write to db
        measurement = "{}_fundingRate".format(self.exchange)
        dbtime = False
        tags = {"symbol": symbol}
        fields = {
            "funding_time": funding_time,
            "interest_rate": interest_rate,
            "funding_rate": funding_rate,
        }
        fields.update({"is_api_return_timestamp": False})
        return self.write_to_measurement(measurement, dbtime, tags, fields)

    # This function is reserved for REST API interaction
    def write_open_interest(self, data, expiration=None):
        # write to db
        measurement = "{}_SwapOpenInterest".format(self.exchange)
        dbtime = data["timestamp"]
        tags = {"symbol": data["instrument_id"], "expiration": expiration}
        fields = {"openInterest": float(data["amount"])}
        fields.update({"is_api_return_timestamp": True})
        return self.write_to_measurement(measurement, dbtime, tags, fields)

    # This function is reserved for REST API interaction
    def write_liquidation(self, data, expiration=None):
        for order in data:
            # write to db
            measurement = "{}_liquidation".format(self.exchange)
            dbtime = order['created_at']
            tags = {
                "symbol": order["instrument_id"],
                "expiration": expiration,
            }
            # all other data should be fields, so delete symbol and timestamp
            fields = copy.copy(order)
            del fields['created_at']
            del fields['instrument_id']
            fields_clean = {}
            fields_clean.update({"loss":float(fields["loss"])})
            fields_clean.update({"price":float(fields["price"])})
            fields_clean.update({"type":int(fields["type"])})
            fields_clean.update({"size":int(fields["size"])})
            fields_clean.update({"is_api_return_timestamp": True})
            success = self.write_to_measurement(measurement, dbtime, tags, fields_clean)
            if not success:
                return False
        return True

    '''
    Functions that interact directly with db
    '''

    def write_to_measurement(self, measurement, time, tags, fields):
        return self.writer.write_points_to_measurement(measurement, time, tags, fields)

    '''
    Error handling
    '''

    # send email to handle error
    def send_email(self, message):
        send(message)

