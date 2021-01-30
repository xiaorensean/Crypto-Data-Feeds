import requests
import websocket
import json
import time



# restful
def get_liqudators():
    query_info = '''
    query Liquidators 
    { liquidation 
    {    id    principalBorrows    currentBorrows    currentBorrowsETH    currentBorrowsUSD    
      reserve {      id      symbol      decimals      __typename    }    
      user {      id      healthFactor      totalBorrowsWithFeesETH      totalCollateralETH      totalCollateralUSD      
            reservesData {        currentUnderlyingBalance       currentUnderlyingBalanceETH        currentUnderlyingBalanceUSD        userBalanceIndex        redirectedBalance        interestRedirectionAddress        
                          reserve {          id          name          symbol          decimals          liquidityRate          lastUpdateTimestamp          reserveLiquidationBonus          usageAsCollateralEnabled          
                                   aToken {            id            __typename          }          
                                   __typename        }        usageAsCollateralEnabledOnUser        borrowRate        borrowRateMode       variableBorrowIndex        lastUpdateTimestamp        __typename      }     
            __typename    }    __typename  }}
    '''
    params ={"operationName": "Liquidators",
    "query": query_info,"variables": {}}
    url = "https://protocol-api.aave.com/graphql"
    response = requests.post(url,data=params)
    data = response.json()
    liquidation_data = data["data"]['liquidation']
    return liquidation_data


# websocket
def connection(message):
    msg_init = {"payload": {},"type": "connection_init"}
    msg_stop = {"payload": {},"type":"stop"}
    ws_url = "wss://api.thegraph.com/subgraphs/name/aave/protocol-raw"
    ws = websocket.create_connection(ws_url)
    ws.send(json.dumps(msg_init))
    ws.send(json.dumps(message))
    response = ws.recv()
    response = ws.recv()
    try:
        data = json.loads(response)
    except:
        pass
    return data

# websocket multy
def connection_multy(message):
    msg_init = {"payload": {},"type": "connection_init"}
    msg_stop = {"payload": {},"type":"stop"}
    ws_url = "wss://api.thegraph.com/subgraphs/name/aave/protocol-multy-raw"
    ws = websocket.create_connection(ws_url)
    ws.send(json.dumps(msg_init))
    ws.send(json.dumps(message))
    response = ws.recv()
    response = ws.recv()
    print(response)
    try:
        data = json.loads(response)
    except:
        pass
    return data

# reservehost update
def get_reserve_rate_hist_update(reserve_address):
    query_info = '''
     subscription ReserveRatesHistoryUpdate($reserveAddress: String!) {
       reserveParamsHistoryItems(where: {reserve: $reserveAddress}, first: 100, orderBy: timestamp, orderDirection: desc) {
        ...ReserveRatesHistoryData
        __typename
           }
            }
        fragment ReserveRatesHistoryData on ReserveParamsHistoryItem {
            variableBorrowRate
            stableBorrowRate
            liquidityRate  utilizationRate
            timestamp
            __typename
            }
              '''
    payload = {"extensions": {}, 
           "operationName": "ReserveRatesHistoryUpdate",
           "query": query_info,
           "variables": {"reserveAddress": reserve_address}}
    msg = {"id":"1","payload":payload,"type":"start"}
    # get websoscket connection
    data = connection(msg)['payload']['data']['reserveParamsHistoryItems']
    return data


def get_reserve_update():
    query_info = '''
    subscription ReserveUpdateSubscription {
        reserves {
            ...ReserveData
            __typename
            }
        }
    
    fragment ReserveData on Reserve {
        id
        name
        symbol
        decimals
        isActive
        usageAsCollateralEnabled
        borrowingEnabled
        stableBorrowRateEnabled
        baseLTVasCollateral
        liquidityIndex
        reserveLiquidationThreshold
        variableBorrowIndex
        aToken {
            id
            __typename
            }
        availableLiquidity
        stableBorrowRate
        liquidityRate
        totalBorrows
        totalBorrowsStable
        totalBorrowsVariable
        totalLiquidity
        utilizationRate
        reserveLiquidationBonus
        variableBorrowRate
        price {
            priceInEth
            __typename
            }
        lastUpdateTimestamp
        __typename
        }
    '''
    payload = {"extensions": {}, 
           "operationName": "ReserveUpdateSubscription",
           "query": query_info,
           "variables": {}}
    msg = {"id":"1","payload":payload,"type":"start"}
    data = connection(msg)['payload']['data']['reserves']
    return data

def get_coin_reserve():
    query_info = '''
    subscription ReservesSubscription($oneHourAgo: Int!, $oneDayAgo: Int!, $oneWeekAgo: Int!, $oneMonthAgo: Int!) {
      items: reserves(orderBy: liquidityRate, orderDirection: desc) {
          ...ReserveItem
              __typename
                }
                }
                
                fragment ReserveHistoryData on ReserveParamsHistoryItem {
                  id
                  timestamp
                    totalLiquidity
                      availableLiquidity
                        totalBorrows
                          totalBorrowsStable
                            totalBorrowsVariable
                              liquidityIndex
                                variableBorrowIndex
                                  __typename
                                  }
                                  
                                  fragment ReserveItem on Reserve {
                                    id
                                      symbol
                                        decimals
                                          totalLiquidity
                                            liquidityRate
                                              variableBorrowRate
                                                stableBorrowRate
                                                  stableBorrowRateEnabled
                                                    availableLiquidity
                                                      totalBorrows
                                                        totalBorrowsStable
                                                          totalBorrowsVariable
                                                            lastUpdateTimestamp
                                                              liquidityIndex
                                                                variableBorrowIndex
                                                                  lifetimeFlashLoans
                                                                    lifetimeLiquidated
                                                                      lifetimeFlashloanProtocolFee
                                                                       lifetimeFlashloanDepositorsFee
                                                                         lifetimeFeeOriginated
                                                                           lifetimeFeeCollected
                                                                             flashLoanHistory(orderBy: amount, orderDirection: desc, first: 1) {
                                                                                 id
                                                                                     amount
                                                                                         __typename
                                                                                          }
                                                                                            price {
                                                                                                id
                                                                                                    priceInEth
                                                                                                        __typename
                                                                                                          }
                                                                                                            pool {
                                                                                                                id    __typename
                                                                                                                  }
                                                                                                                    __typename
                                                                                                                    }
                                                                                                                    
                 '''
    current_time = int(time.time())
    payload = { "extensions": {},
                "operationName": "ReservesSubscription",
                "query": query_info,
                "variables": {"oneHourAgo": current_time-60*60, "oneDayAgo": current_time-60*60*24, "oneWeekAgo": current_time-60*60*24*7, "oneMonthAgo": current_time-60*60*24*7*4}
                }
    msg = {"id": "2", "payload": payload, "type": "start"}
    data = connection(msg)['payload']['data']['items']
    return data

def get_priceOracle():
    query_info = '''
    subscription PriceOracle {
      priceOracle(id: 1) {
          usdPriceEth
              lastUpdateTimestamp
                  __typename
                    }
                    }
                 '''
    payload = {
        "extensions": {},
        "operationName": "PriceOracle",
        "variables": {},
        "query":query_info
    }
    msg = {"id": "3", "payload": payload, "type": "start"}
    data = connection(msg)['payload']['data']
    return data

def get_summary():
    query_info = '''
    {
      burn {
          totalBurnedLEND
              totalReadyToBurnLEND
                  lendPrice
                      marketCap
                          volume24Hours
                              __typename
                                }
                                }
    '''
    graphql = "https://protocol-api.aave.com/graphql"
    payload = {"query": query_info, "variables": {}}
    repsone = requests.post(graphql, data=payload)
    data = repsone.json()
    return data

def get_flashLoan():
    query_info = """"
    subscription FlashLoans($limit: Int!) {
      items: flashLoans(orderBy: timestamp, orderDirection: desc, first: $limit) {
          ...FlashLoan
              __typename
                }
                }
                fragment FlashLoan on FlashLoan {
                  id
                    timestamp
                      amount
                        reserve {
                            symbol
                                decimals
                                    __typename
                                      }
                                        pool {
                                            id
                                                __typename
                                                  }
                                                    __typename
                                                    }
    """
    msg = {"id": "1",
          "payload": {"variables": {"limit": 10},
                      "extensions": {},
                      "operationName": "FlashLoans",
                      "query": query_info},
          "type": "start"
          }
    data = connection_multy(msg)
    return data

def get_liquidation():
    msg = {"id": "5",
           "payload": {"variables": {"limit": 10},
           "extensions": {},
           "operationName": "Liquidations",
           "query": "subscription Liquidations($limit: Int!) {  items: liquidationCalls(orderBy: timestamp, orderDirection: desc, first: $limit) {    ...Liquidation    __typename  }}  fragment Liquidation on LiquidationCall {  id  timestamp  collateralAmount  collateralReserve {    symbol    decimals    __typename  }  principalAmount↵  principalReserve {    symbol    decimals    __typename  }  pool {    id    __typename↵  }  __typename}",
           "variables": {"limit": 10}},
           "type": "start"}
    data = connection_multy(msg)
    return data

def get_deposit():
    pass

def get_withdraw():
    pass

def get_borrow():
    pass

def get_repay():
    pass

if __name__ == "__main__":
    #dai_token = "0x6b175474e89094c44da98b954eedeac495271d0f"
    #data = get_reserve_rate_hist_update(dai_token)
    #data = get_reserve_update()
    data = get_coin_reserve()
    data = get_summary()
    print(data["errors"])
    #for d in data:
        #print(d)
    #data = get_priceOracle()
    #data = get_flashLoan()
    #data = get_liquidation()
    print(data)





