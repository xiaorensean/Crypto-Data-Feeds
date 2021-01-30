# FTX Market Data
## API Document
REST endpoint URL: https://ftx.com/api  
### Futures/List all Futures
```
GET /futures
```
### Futures/Get funding rates
```
GET /funding_rates
```
### Futures/ Get future stats
```
GET /futures/{future_name}/stats
```
- Parameters
  - future_name
    - Type: String
    - Value: BTC0628
    - Description: name of the token   
### Market/Get Trades
Rest endpoint URL: 
```
GET /markets/{market_name}/trades?limit={limit}&start_time={start_time}&end_time={end_time}
```
- Parameters
  - market_name
    - Type: String
    - Value: BTC0628
    - Description: name of rhe token 
  - limit
    - Type: Number 
    - Value: 35
    - Description: default/min 20, max: 100 
  - start_time
    - Type: number
    - Value: 1559881511
    - Description: start timestamp
  - end_time
    - Type: number
    - Value: 1559881711
    - Description: end timestamp

## Data upadting logic 
### Trades Data
Trade data api has start time and end time parameters and by it can fetch full trade history by setting a far-away start time and set current timestamp as end time. There are two funcions -- __write_historical_trade_data__ and __write_recent_trade_data__ 
Since contract names will frequently updated so we need to double check for new contract name for each updating. Next the process is to initially fetch historical trades data, then to update trades data in every 10 minutes. 
   
