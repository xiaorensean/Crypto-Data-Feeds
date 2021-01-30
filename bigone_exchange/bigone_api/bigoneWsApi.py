import websocket
import json


def ws_connection(msg):
    endpoint = 'wss://big.one/ws/v2'
    ws = websocket.create_connection(endpoint)
    ws.send(json.dumps(msg))
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    
    return data



def msg(method,params):
    msg = \
        {
            "requestId": "1",
            method:{"market":params}
        }
        
    return msg



def ws_market_depth(symbol):
    message = msg("subscribeMarketDepthRequest",symbol)
    response = ws_connection(message)
    return response



if __name__ == "__main__":
    data = ws_market_depth("BTC-USD")
    