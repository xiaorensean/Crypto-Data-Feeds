import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from bitfinex_api.BfxRest import BITFINEXCLIENT


# bitfinex client
API_KEY = "API_KEY"
API_SECRETE = "API_SECRETE"
bitfinex =  BITFINEXCLIENT(API_KEY,API_SECRETE)


# get all tickers
def get_all_tickers(options):
    # fetch all tickers with funding trades 
    tickers = bitfinex.get_public_tickers("ALL")
    if tickers is None:
        print ("Server Error!")
        return 
    else:
        pass
    funding_tickers = []  
    trading_tickers = []
    for t in tickers:
        ticker = t[0]
        if ticker[0] == 'f':
            funding_tickers.append(ticker)
        if ticker[0] == 't':
            trading_tickers.append(ticker)
        else:
            pass
    if options == 'funding':
        return funding_tickers
    else:
        return trading_tickers
