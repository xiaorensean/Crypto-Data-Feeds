import os
import sys
import requests

BASE_URL = "https://hashpool.com/api/"

def get_pool_coins():
    endpoint = BASE_URL + "coins"
    response = requests.get(endpoint)
    data = response.json()
    return data

def get_coin_blocks(coin):
    endpoint = BASE_URL + "blocks/{}?offset=0&limit=50".format(coin)
    response = requests.get(endpoint)
    data = response.json()
    return data


if __name__ == "__main__":
    data = get_pool_coins()
    