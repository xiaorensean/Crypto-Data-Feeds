import requests
import os
import sys

def get_coinmarket_cap_price(symbol):
    url = "https://coinmarketcap-api.synthetix.io/public/prices?symbols={}".format(symbol)
    response = requests.get(url)
    data = response.json()
    return data


def get_open_interest():
    url = "https://api.synthetix.io/api/exchange/openInterest"
    response = requests.get(url)
    data = response.json()
    return data

def synthetix():
    url = "https://api.thegraph.com/subgraphs/name/synthetixio-team/synthetix"
    response = requests.get(url)
    data = response.json()
    return data

def synthetix_exchange():
    url = "https://api.thegraph.com/subgraphs/name/synthetixio-team/synthetix-exchanges"
    response = requests.get(url)
    data = response.json()
    return data

def synth_chart():
    url = "https://api.synthetix.io/api/dataPoint/chartData"
    response = requests.get(url)
    data = response.json()
    return data

def synth_dahsboard_chart():
    url = "https://api.synthetix.io/api/dataPoint/dashboardData"
    response = requests.get(url)
    data = response.json()
    return data


if __name__ == "__main__":
    #price = get_coinmarket_cap_price("SNX")
    open_interest = get_open_interest()