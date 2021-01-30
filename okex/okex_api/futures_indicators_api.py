import requests
import datetime 
import os
import sys 
sys.path.append(os.path.join(os.path.dirname(__file__)))


# get request http api 
def get(url):
    response = requests.get(url)
    resp = response.json()
    if resp['code'] == 0:
        data = resp['data']
    else:
        data = None
        print("Api error!")
    return data


# list all tickers
def all_tickers():
    symbols = ["BTC","LTC","ETH","ETC","XRP","EOS","BSV","BCH","TRX"]
    return symbols


# Long/Short Users Ratio
# 1 parameters freq:
# freq: '0' 5 minutes
# freq: '1' 1 hours
# freq: '2' 1 day

# 2 parameter asset
# assst: BTC
# asset: ETH
# asset: LTC
def longShortPositionRatio(symbol,freq):
    if freq == 5:
        URL = 'https://www.okex.com/v3/futures/pc/market/longShortPositionRatio/{}?currency=BTC&unitType=0'.format(symbol)
    if freq == 60:
        URL = 'https://www.okex.com/v3/futures/pc/market/longShortPositionRatio/{}?currency=BTC&unitType=1'.format(symbol)
    if freq == 1440:
        URL = 'https://www.okex.com/v3/futures/pc/market/longShortPositionRatio/{}?currency=BTC&unitType=2'.format(symbol)
    data = get(URL)
    return data



# parameters
# 1 contractType
# this_week(weekly) & next_week(biweekly) & quarte
def futureBasis(freq,symbol,contract_type):
    # swap data
    if contract_type == 'swap':
        if freq == 1:
            siURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=60&size=1000'.format(symbol)
            spURL = 'https://www.okex.com/v2/perpetual/pc/public/instruments/{}-USD-SWAP/candles?granularity=60&size=1000'.format(symbol)
        elif freq == 5:
            siURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=300&size=1000'.format(symbol)
            spURL = 'https://www.okex.com/v2/perpetual/pc/public/instruments/{}-USD-SWAP/candles?granularity=300&size=1000'.format(symbol)
        elif freq == 60:
            siURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=3600&size=1000'.format(symbol)
            spURL = 'https://www.okex.com/v2/perpetual/pc/public/instruments/{}-USD-SWAP/candles?granularity=3600&size=1000'.format(symbol)
        elif freq == 1440:
            siURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=86400&size=1000'.format(symbol)
            spURL = 'https://www.okex.com/v2/perpetual/pc/public/instruments/{}-USD-SWAP/candles?granularity=86400&size=1000'.format(symbol)
        # convert for futures
        data_swap_price_raw = get(spURL)
        data_swap_index_raw = get(siURL)
        length = 1000
        min_data = min(len(data_swap_price_raw),len(data_swap_index_raw))
        if  min_data < length:
            length = min_data
        else:
            length = 1000
        data = []
        for idx in range(length):
            data.append([data_swap_price_raw[-idx][0],float(data_swap_price_raw[-idx][4]),float(data_swap_index_raw[-idx][4]),(float(data_swap_price_raw[-idx][4])-float(data_swap_index_raw[-idx][4])),])
        data_sorted = sorted(data, key=lambda data: data[0],reverse=False)
    # futures data
    else:
        if freq == 1:
            fURL = 'https://www.okex.com/v2/futures/pc/market/klineData.do?symbol=f_usd_{}&type=1min&limit=1000&coinVol=1&contractType={}'.format(symbol.lower(),contract_type)
            sURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=60&size=1000'.format(symbol)
        elif freq == 5:
            fURL = 'https://www.okex.com/v2/futures/pc/market/klineData.do?symbol=f_usd_{}&type=5min&limit=1000&coinVol=1&contractType={}'.format(symbol.lower(),contract_type)
            sURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=300&size=1000'.format(symbol)
        elif freq == 60:
            fURL = 'https://www.okex.com/v2/futures/pc/market/klineData.do?symbol=f_usd_{}&type=1hour&limit=1000&coinVol=1&contractType={}'.format(symbol.lower(),contract_type)
            sURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=3600&size=1000'.format(symbol)
        elif freq == 1440:
            fURL = 'https://www.okex.com/v2/futures/pc/market/klineData.do?symbol=f_usd_{}&type=day&limit=1000&coinVol=1&contractType={}'.format(symbol.lower(),contract_type)
            sURL = 'https://www.okex.com/api/index/v3/instruments/{}-USD/candles?granularity=86400&size=1000'.format(symbol)
        # convert for futures
        data_future_price_raw = get(fURL)
        data_future_index_raw = get(sURL)
        data_future_price = [[str(datetime.datetime.utcfromtimestamp(i[0]/1000)),i[4]] for i in data_future_price_raw]
        data_future_index = [[i[0].replace("T"," ").split(".")[0],float(i[4])] for i in data_future_index_raw]
        length = 1000
        min_data = min(len(data_future_price),len(data_future_index))
        if  min_data < length:
            length = min_data
        else:
            length = 1000
        data = []
        for idx in range(length):
            data.append([data_future_price[idx][0],data_future_price[idx][1],data_future_index[idx][1],(data_future_price[idx][1]-data_future_index[idx][1])])
        data_sorted = sorted(data, key=lambda data: data[0],reverse=False)
    return data_sorted


# funding rates
def SwapFundingRates(symbol,intervel):
    url = "https://www.okex.com/v2/perpetual/pc/public/calculate/fundingRate?contract={}&interval={}".format(symbol,intervel)
    response = requests.get(url)
    resp = response.json()
    return resp



# BTC Futures Open Interest and Trading Volume
# 1 parameters freq:
# freq: '0' 5 minutes
# freq: '1' 1 hours
# freq: '2' 1 day
    
# 2 parameter asset
# assst: BTC
# asset: ETH
# asset: LTC
def FuturesOpenInterestAndTradingVolume(asset,freq):
    if freq == 5:
        URL = 'https://www.okex.com/v3/futures/pc/market/openInterestAndVolume/{}?unitType=0'.format(asset)
    if freq == 60:
        URL = 'https://www.okex.com/v3/futures/pc/market/openInterestAndVolume/{}?unitType=1'.format(asset)
    if freq == 24:
        URL = 'https://www.okex.com/v3/futures/pc/market/openInterestAndVolume/{}?unitType=2'.format(asset)
    response = requests.get(URL)
    resp = response.json()
    return resp




# BTC Futures Taker Buy and Sell
def FuturesTakerBuyAndSell(asset,freq):
    if freq == 5:
        URL = 'https://www.okex.com/v3/futures/pc/market/takerTradeVolume/{}?unitType=0'.format(asset)
    if freq == 60:
        URL = 'https://www.okex.com/v3/futures/pc/market/takerTradeVolume/{}?unitType=1'.format(asset)
    if freq == 24:
        URL = 'https://www.okex.com/v3/futures/pc/market/takerTradeVolume/{}?unitType=2'.format(asset)
    response = requests.get(URL)
    resp = response.json()
    return resp



# BTC Top Trader Sentimental Index
def TopTraderSentimentalIndex(asset,freq):
    if freq == 5:
        URL = 'https://www.okex.com/v2/futures/pc/public/eliteScale.do?symbol=f_usd_{}&type=0'.format(asset.lower())
    if freq == 15:
        URL = 'https://www.okex.com/v2/futures/pc/public/eliteScale.do?symbol=f_usd_{}&type=1'.format(asset.lower())
    if freq == 60:
        URL = 'https://www.okex.com/v2/futures/pc/public/eliteScale.do?symbol=f_usd_{}&type=2'.format(asset.lower())
    response = requests.get(URL)
    resp = response.json()
    return resp


# BTC Top trader Average Margin Used
def TopTraderAverageMarginUsed(symbol,freq):
    if freq == 5:
        URL = 'https://www.okex.com/v2/futures/pc/public/getFuturePositionRatio.do?symbol=f_usd_{}&type=0'.format(symbol.lower(),freq)
    if freq == 15:
        URL = 'https://www.okex.com/v2/futures/pc/public/getFuturePositionRatio.do?symbol=f_usd_{}&type=1'.format(symbol.lower(),freq)
    if freq == 60:
        URL = 'https://www.okex.com/v2/futures/pc/public/getFuturePositionRatio.do?symbol=f_usd_{}&type=2'.format(symbol.lower(),freq)
    response = requests.get(URL)
    resp = response.json()
    return resp









if __name__ == '__main__':
    senti_index = TopTraderSentimentalIndex("BTC",60)
    senti_index_data = senti_index['data']
    


