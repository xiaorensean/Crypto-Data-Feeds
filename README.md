# Market-Data
- Server: 
   - HOST_1: 35-182-242-71.ca-central-1.compute.amazonaws.com
- This is a library with scripts to pull futures and perpetual swaps data from different exchanges, and then writes into an InfluxDB database. Currently, the main features of interest being collected are price, volume, open interest, and funding rate. However, all other metadata that can be pulled from the exchanges are also being stored; this data differs from exchange to exchange.

### Dependencies
```
pip3 install pandas
pip3 install influxdb
pip3 install bitmex
pip3 install termcolor
pip3 install cryptofeed
```

### Installation and Running
- Install all dependencies
- Clone this repository into a remote machine (ec2)
- In `resubscribe.py`, check that line 6 calls `python3` and not `python`
  - This is because we need to run the scripts explicitly with Python3
- In `monitor.py`, ensure that the list of `scripts`contains all the scripts you wish to run continuously
- In `influxdb_client/influxdb_writer.py` select the server where you want to write to
- To run script in background continuously: `nohup python3 monitor.py &`

### Functionality
Bitmex
  - All XBT and ETH swaps and futures are being tracked
  - Price, high, low, including futures premium/discount to swaps
  - All Trades
  - Open Interest
  - Funding Rate (Every 8 hours)
  - Indicative funding rate
  - Volume, 24h Volume
  - Liquidations
  - Settlements
  - Insurance

Okex
  - All XBT, ETH, LTC swaps and futures are being tracked
  - Price, high, low, including futures premium/discount to swaps
  - All Trades
  - Open Interest
  - Funding Rate (Constantly updated)
  - Volume, 24h Volume
  - Liquidations

Deribit
  - All XBT and ETH swaps and futures are being tracked
  - Price, high, low, including futures premium/discount to swaps
  - All Trades
  - Open Interest
  - Funding Rate (Constantly updated)
  - Volume, 24h Volume
  
### Supported Exchanges
Bitmex
- Swap data
- Futures data
- Order books

Okex
- Swap data
- Futures data
- Order books

Deribit
- Swap data
- Futures data
- Order books

Bitfinex
- Order book for lending and borrowing spot

### Completed scripts
monitor.py
resubscribe.py
- bitmex
  - subscribe_instrument.py
  - subscribe_slow_metadata.py
  - subscribe_trade_data.py
  - subscribe_cryptofeed_trades.py
  - metadata_functions.py
- okex
  - subscribe_data.py
  - subscribe_oi_swaps.py
  - subscribe_liquidation.py
  - metadata_functions.py
- deribit
  - subscribe_swap_data.py
  - subscribe_futures_data.py
  - subscribe_options_data.py
  - subscibe_btc_trades.py
  - subscribe_eth_trades.py
  - metadata_functions.py
- influxdb_client
  - bitmex_influxdb_client.py
  - okex_influxdb_client.py
  - deribit_influxdb_client.py
  - influx_writer.py
  - emails.py
- checkdb
  - check_metadata.py
  - check_metadata2.py

### Work in progress
- deribit options, too many websockets at once
- Need to restart all scripts on Friday at 8:00AM UTC
- order books, to coordinate checksums
