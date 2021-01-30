import os
import sys
import requests


def get_bitcoin_futures_index():
    url = "https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/8478/G?quoteCodes=null"
    response = requests.get(url)
    data = response.json()
    return data

if __name__ == "__main__":
    data = get_bitcoin_futures_index()