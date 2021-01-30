import websocket
import json


def connection(msg):
    endpoint = 'wss://www.deribit.com/ws/api/v2'
    ws = websocket.create_connection(endpoint)
    ws.send(json.dumps(msg))
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    
    return data

def subscription(msg):
    endpoint = 'wss://www.deribit.com/ws/api/v2'
    ws = websocket.create_connection(endpoint)
    ws.send(json.dumps(msg))
    # subscribing successfully
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    # receiving data
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    
    return data

def message(method,params):
    msg = \
        {
          "jsonrpc" : "2.0",
          "id" : 9290,
          "method" : method,
          "params" : params
        }
        
    return msg

def get_last_trades_by_currency(symbol,trade_history):
    method = "public/get_last_trades_by_currency"
    params = { "currency" : symbol,
               "count" : trade_history}
    msg = message(method,params)
    data = connection(msg)['result']['trades']

    return data


def get_orderbook(symbol,depth):
    method = "public/get_order_book"
    params = { "instrument_name": symbol,
               "depth": depth}
    msg = message(method,params)
    data = connection(msg)['result']

    return data

def subscribe_orderbook_data(symbol):
    method = "public/subscribe"
    params = { "channels": [symbol]}
    msg = message(method,params)
    data = subscription(msg)['params']['data']

    return data

def get_index(symbol):
    method = "public/get_index"
    params = {"currency": symbol}
    msg = message(method,params)
    data = connection(msg)['result']
    return data


if __name__ == "__main__":
    orderbook1 = subscribe_orderbook_data("book.ETH-PERPETUAL.none.20.100ms")
    orderbook = get_orderbook("ETH-PERPETUAL",200)

