# Bitmex Market Data

## Bitmex Funding Rates

### Data Consistency: 
- Lose data from 2019-08-16 14:27:00 to 2019-08-16 14:58:00 UTC
  - Data may be some error in the following date time:
     - 2019-08-16 15:47:28.490633: XBTUSD lose 1 minute 
     - 2019-08-16 15:49:32.193034: XBTUSD lose 1 minute 
     - 2019-08-16 15:50:35.435400: XBTUSD lose 1 minute
     - 2019-08-18 16:20:13.540362: EYHUSD lose 1 minute
     - 2019-08-27 20:30:00 - 2019-08-27 20:33:00 ETHUSD and BTCUSD 
  - Data Loss
     - 2019-10-08 08:22:59.000000 - 2019-10-08 21:52:47.000000: XBTUSD
     - 2019-10-08 08:22:59.000000 - 2019-10-08 21:52:47.000000: XBTUSD

### Error Type
- influxDBClientError 
influxdb.exceptions.InfluxDBClientError: 400: 
{"error":"partial write: field type conflict: input field \"indicativeFundingRate\" on measurement \"bitmex_funding_rates\" 
is type integer, already exists as type float dropped=1"}
indicativeFundingRate supposed to be float but int instead. 

- Expecting Value 
api request failure URL error caused Json format inconvertable  

## Bitmex Leaderboard 
### Scheduler for updating
It is updating for every hour since api updating time is random. 
