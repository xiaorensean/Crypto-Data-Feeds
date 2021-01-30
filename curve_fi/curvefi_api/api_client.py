import os
import sys
import requests

token_list = ["compound", "usdt", "y", "busd", "susd", "pax", "ren2", "rens"]
freq_list = ["1", "5", "10", "30", "1440"]

def get_hist_data(token,freq):
    url_base = "https://curve.fi/raw-stats/{}-{}m.json".format(token,freq)
    response = requests.get(url_base)
    data = response.json()
    return data

def get_apy_pool(pool):
    url = "http://pushservice.curve.fi/apys/{}".format(pool)
    response = requests.get(url)
    data = response.json()
    return data


def get_pool():
    url_base = "http://pushservice.curve.fi/pools"
    response = requests.get(url_base)
    data = response.json()
    return data

if __name__ == "__main__":
    data = get_hist_data("susd","1")
    vol = [d['volume'] for d in data]
    price = [d['prices'] for d in data]
    print(vol)
    print(price)
    print(get_pool())
