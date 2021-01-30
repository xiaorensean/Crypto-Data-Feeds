import requests
import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(current_dir))

pkg_dir = os.path.dirname(current_dir)



## api 
def get_difficulty(start,end):
    url = "https://beta.urkellabs.com/charts/difficulty?endTime={}&startTime={}".format(start,end)
    response = requests.get(url)
    resp = response.json()
    return resp

def get_daily_tx(start,end):
    url = "https://beta.urkellabs.com/charts/dailyTransactions?endTime={}&startTime={}".format(start,end)
    response = requests.get(url)
    resp = response.json()
    return resp

def get_daily_totaltx(start,end):
    url = "https://beta.urkellabs.com/charts/dailyTotalTransactions?endTime={}&startTime={}".format(start,end)
    response = requests.get(url)
    resp = response.json()
    return resp

def get_supply(start,end):
    url = "https://beta.urkellabs.com/charts/supply?endTime={}&startTime={}".format(start,end)
    response = requests.get(url)
    resp = response.json()
    return resp

def get_burned(start,end):
    url = "https://beta.urkellabs.com/charts/burned?endTime={}&startTime={}".format(start,end)
    response = requests.get(url)
    resp = response.json()
    return resp

## API no longer available
def get_summary():
    url = "https://beta.urkellabs.com/summary?"
    response = requests.get(url)
    resp = response.json()
    return resp
def get_status():
    url = "https://beta.urkellabs.com/status/?"
    response = requests.get(url)
    resp = response.json()
    return resp

if __name__ == "__main__":
    pass