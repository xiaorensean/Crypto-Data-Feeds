import time
import json
import requests
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def domain_popular(page):
    popular_endpoint = "https://www.namebase.io/api/domains/popular/{}".format(page)
    response = requests.get(popular_endpoint)
    data = response.json()
    return data


def domain_ending_soon(page):
    ending_soon_endpoint = "https://www.namebase.io/api/domains/ending-soon/{}".format(page)
    response = requests.get(ending_soon_endpoint)
    data = response.json()
    return data


def domain_anticipated(page):
    anticipated_endpoint = "https://www.namebase.io/api/domains/anticipated/{}".format(page)
    response = requests.get(anticipated_endpoint)
    data = response.json()
    return data


def domain_recently_sold(page):
    recently_sold_endpoint = "https://www.namebase.io/api/domains/recently-sold/{}".format(page)
    response = requests.get(recently_sold_endpoint)
    data = response.json()
    return data


if __name__ == "__main__":
    a0 = domain_popular(0)
    a1 = domain_popular(1)
    a4667 = domain_popular()
