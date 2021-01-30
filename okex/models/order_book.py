"""
Module used to describe all of the different data types
"""

import zlib
import json
import time

class OrderBook:
    """
    Object used to store the state of the orderbook. This can then be used
    in one of two ways. To get the checksum of the book or so get the bids/asks
    of the book
    """

    def __init__(self):
        self.asks = []
        self.bids = []

    def get_bids(self):
        """
        Get all of the bids from the orderbook

        @return bids Array
        """
        return self.bids

    def get_asks(self):
        """
        Get all of the asks from the orderbook

        @return asks Array
        """
        return self.asks

    def update_from_snapshot(self, data):
        """
        Update the orderbook with a raw orderbook snapshot
        """
        try:
            self.bids = data["bids"]
        except:
            pass
        try:
            self.asks = data["asks"]
        except:
            pass

    def update_with(self, data):
        """
        Update the orderbook with a single update
        """
        
        bids = data["bids"]
        asks = data["asks"]

        #!! Note everything here is formatted as a string

        # match price level for bids
        for new_bid in bids:
            new_price = new_bid[0]
            quantity = new_bid[3]
            price_match_found = False
            for record in self.bids:
                recorded_price = record[0]
                if new_price == recorded_price:
                    price_match_found = True
                    if quantity == "0":
                        self.bids.remove(record)
                    else:
                        self.bids.remove(record)
                        self.bids.append(new_bid)
                    break
                #if price is not already found, add it 
            if not price_match_found:
                self.bids.append(new_bid)

        for new_ask in asks:
            new_price = new_ask[0]
            quantity = new_ask[3]
            price_match_found = False
            for record in self.asks:
                recorded_price = record[0]
                if new_price == recorded_price:
                    price_match_found = True
                    if quantity == "0":
                        self.asks.remove(record)
                    else:
                        self.asks.remove(record)
                        self.asks.append(new_ask)
                    break
                #if price is not already found, add it 
            if not price_match_found:
                self.asks.append(new_ask)

        # #write to csv
        # if (old != None):
        #     old_quantity = abs(old[0][2])
        #     new_quantity = abs(zip_order[0][2])
        #     if (new_quantity <= old_quantity):
        #         isBid = -1
        #         if (side == self.bids):
        #             isBid = 1
        #         else:
        #             isBid = 0
        #         line = "{},{},{}".format(time.time(), zip_order[1][0], isBid)
        #         with open(symbol + ".csv", "a") as f:
        #             f.write(line + "\n")

        # sort book
        self.bids.sort(key = lambda x:x[0], reverse = True)
        self.asks.sort(key = lambda x:x[0], reverse = False)

        # print (self.bids)
        # print (self.asks)

        return

    def checksum(self):
        """
        Generate a CRC32 checksum of the orderbook
        """
        data = []
        # take set of top 25 bids/asks
        for index in range(0, 25):
            if index < len(self.bids):
                # use the string parsed array
                bid = self.bids[index]
                price = bid[0]
                amount = bid[1]
                data += [price]
                data += [amount]
            if index < len(self.asks):
                # use the string parsed array
                ask = self.asks[index]
                price = ask[0]
                amount = ask[1]
                data += [price]
                data += [amount]
        checksum_str = ':'.join(data)
        # calculate checksum and force signed integer
        checksum = zlib.crc32(checksum_str.encode('utf8')) & 0xffffffff

        return checksum
