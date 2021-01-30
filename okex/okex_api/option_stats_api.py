import requests

def btc_options_oi_tradingVol(freq,symbol="BTC-USD"):
    if freq == 8*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/positionAndVolume/{}?unitType=1".format(symbol)
    elif freq == 24*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/positionAndVolume/{}?unitType=2".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data

def btc_options_oi_tradingVol_strike(freq,delivery_time="20200619",symbol="BTC-USD"):
    if freq == 8*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/strike/positionAndVolume/{}?unitType=1&delivery_time={}".format(symbol,delivery_time)
    elif freq == 24*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/strike/positionAndVolume/{}?unitType=2&delivery_time={}".format(symbol,delivery_time)
    response = requests.get(url)
    data = response.json()['data']
    return data

def btc_options_oi_tradingVol_expiry(freq,symbol="BTC-USD"):
    if freq == 8*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/delivery/positionAndVolume/{}?unitType=1".format(symbol)
    elif freq == 24*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/delivery/positionAndVolume/{}?unitType=2".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data

def btc_options_call_put_ratios(freq):
    data = btc_options_oi_tradingVol(freq)
    oi_ratio = []
    vol_ratio = []
    for idx in range(len(data['call_position'])):
        oi_ratio += [data['call_position'][idx]/data['put_position'][idx]]
        vol_ratio += [data['call_volume'][idx]/data['put_volume'][idx]]
    ratio = {}
    ratio.update({"open_interest_ratio":oi_ratio})
    ratio.update({"trading_volume_ratio":vol_ratio})
    ratio.update({"timestamps":data['timestamps']})
    return ratio

def btc_options_implied_vol(delivery_time,symbol='BTC-USD'):
    url = "https://www.okex.com/v3/option/pc/public/market/vol/{}?delivery_time={}".format(symbol,delivery_time)
    response = requests.get(url)
    data = response.json()['data']
    return data

def btc_options_atm_vol(symbol="BTC-USD"):
    url = "https://www.okex.com/v3/option/pc/public/market/atmVol/{}".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data

# distance
# 1, 2, 3
def btc_options_implied_vol_skew(distance):
    if distance == 0.02:
        url = "https://www.okex.com/v3/option/pc/public/market/heeling/BTC-USD?distance=1"
    if distance == 0.05:
        url = "https://www.okex.com/v3/option/pc/public/market/heeling/BTC-USD?distance=2"
    if distance == 0.10:
        url = "https://www.okex.com/v3/option/pc/public/market/heeling/BTC-USD?distance=3"
    response = requests.get(url)
    data = response.json()['data']
    return data

def btc_options_impliedVol_histVol(symbol="BTC-USD"):
    url = "https://www.okex.com/v3/option/pc/public/market/impAndHisVol/{}".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data


def btc_options_takerflow(freq,symbol="BTC-USD"):
    if freq == 8*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/takerTradeVolume/{}?unitType=1".format(symbol)
    if freq == 24*60*60:
        url = "https://www.okex.com/v3/option/pc/public/market/takerTradeVolume/{}?unitType=1".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data


def btc_order_scatter(symbol="BTC-USD"):
    url = "https://www.okex.com/v3/option/pc/public/market/tradeVolume/{}".format(symbol)
    response = requests.get(url)
    data = response.json()['data']
    return data






if __name__ == "__main__":
    #data = btc_options_oi_vol(8*60*60)
    #data = btc_options_oi_tradingVol_strike(24*60*60)
    #data = btc_options_oi_tradingVol_expiry(24*60*60)
    #data = btc_options_implied_vol("20200619")
    #data = btc_options_implied_vol_skew("3")
    #data = btc_options_oi_tradeVolume()
    #data = btc_options_oi_tradingVol(8*60*60)
    #print(data)
    data = btc_options_call_put_ratios(8*60*60)
    print(data)



