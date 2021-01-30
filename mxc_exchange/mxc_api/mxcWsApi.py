import websocket
import json


def connection(msg):
    endpoint = 'wss://wbs.mxc.com'
    ws = websocket.create_connection(endpoint)
    ws.send(json.dumps(msg))
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    
    return data

def get_trade():
    pass