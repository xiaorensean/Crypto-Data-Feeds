import requests
import websocket
import json
import time

headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36" }


def get_exchange_rate():
    query_info = '''
    {
      query LatestExchangeRate {
        exchangeRates(first: 1, orderDirection: desc, orderBy: timestamp) {
            exchangeRate
                timestamp
                    __typename
                      }
                      }
    '''
    graphql = "https://api.thegraph.com/subgraphs/name/mstable/mstable-protocol"
    payload = {"operationName": "exchangeRates", "query": "query LatestExchangeRate {  exchangeRates(first: 1, orderDirection: desc, orderBy: timestamp) {    exchangeRate    timestamp    __typename  }}",
               "variables": {}}
    repsone = requests.post(graphql, data=payload, headers=headers)
    print(repsone)
    #data = repsone.json()
    #return data


if __name__ == "__main__":
    data = get_exchange_rate()
    print(data)





