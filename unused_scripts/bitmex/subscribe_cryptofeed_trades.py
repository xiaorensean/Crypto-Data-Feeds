'''
Uses cryptofeed to subscribe to trades for all active symbols
'''

from cryptofeed.callback import TickerCallback, TradeCallback, BookCallback, FundingCallback, InstrumentCallback
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Bitmex, Coinbase, Bitfinex, Poloniex, Gemini, HitBTC, Bitstamp, Kraken, Binance, EXX, Huobi, HuobiUS, OKCoin, OKEx, Coinbene
from cryptofeed.defines import L3_BOOK, L2_BOOK, BID, ASK, TRADES, TICKER, FUNDING, COINBASE, INSTRUMENT

import datetime as dt
import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) )))
from influxdb_client.bitmex_influxdb_client import BitmexInfluxClient

client = BitmexInfluxClient()

async def trade(feed, pair, order_id, timestamp, side, amount, price):
	if not client.cryptofeed_trade(timestamp, pair, side, order_id, price, amount):
		sys.exit(1)

async def instrument(**kwargs):
    # print(f"Instrument update: {kwargs}")
    pass

def main():
    f = FeedHandler() 

    bitmex_symbols = Bitmex.get_active_symbols()
    f.add_feed(Bitmex(channels=[TRADES], pairs=bitmex_symbols, callbacks={TRADES: TradeCallback(trade)}))

    f.run()

if __name__ == '__main__':
    main()