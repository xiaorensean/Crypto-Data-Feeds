import requests
import websocket
import json
import time


# websocket
def connection(message):
    msg_init = {"payload": {},"type": "connection_init"}
    msg_stop = {"payload": {},"type":"stop"}
    ws_url = "wss://graphql-wm-lb.bandchain.org/v1/graphql"
    ws = websocket.create_connection(ws_url)
    # send
    ws.send(json.dumps(msg_init))
    response = ws.recv()
    print(response)
    ws.send(json.dumps(message))
    response = ws.recv()
    response = ws.recv()
    response = ws.recv()
    try:
        data = json.loads(response)['payload']['data']
    except:
        pass
    return data


def get_validators():
    query_info = '''
    subscription Validators($jailed: Boolean!) {
      validators(where: {jailed: {_eq: $jailed}}, order_by: {tokens: desc, moniker: asc}) {
          operatorAddress: operator_address
              consensusAddress: consensus_address
                  moniker
                      identity
                          website
                              tokens
                                  commissionRate: commission_rate
                                      commissionMaxChange: commission_max_change
                                          commissionMaxRate: commission_max_rate
                                              consensusPubKey: consensus_pubkey
                                                  jailed
                                                      details
                                                          oracleStatus: status
                                                              __typename
                                                                }
                                                                }
              '''
    payload = {"extensions": {},
           "operationName": "Validators",
           "query": query_info,
           "variables": {"jailed": False}}
    msg = {"id":"1000","payload":payload,"type":"start"}
    # get websoscket connection
    data = connection(msg)
    return data

if __name__ == "__main__":
    data = get_validators()
    print(data)
