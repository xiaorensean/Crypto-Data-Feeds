# Market-Data
This libray is to collect maket data for different exchanges, and then write into influxdb database. Currently the main interest of data collecting is trades, funding rates, open interest, futures contracts related indicators. The data format is different by exchange.

### Script running
Two screen runing to write into different database(**_screen id may change_**)
- **_manually change 'HOST_1' or 'HOST_2' in influxdb_client.py at line 20_**
- HOST_1: 24315.pts-0.ip-172-31-15-174
- HOST_2: 28308.pts-0.ip-172-31-15-174
- Monitor_h_1: 24568.pts-0.ip-172-31-15-174
- Monitor_h_2:22199.pts-0.ip-172-31-15-174


### Functionality
Bitmex
- Funding Rates (indicative and normal)
- Leaderboard (ROE and notional)

FTX
- All Trades
- Funding Rates
- Open Interest

OKEX
- Futures Long Short Ratios (BTC, ETH, LTC)

### Completed Scripts
subscribe_data.py
- bitmex
  - subscribe_funding_rates.py
  - subscribe_leaderboard.py
- ftx
  - ftx_api.py
  - subscribe_trades.py
  - subscribe_funding_rates.py
  - subscribe_future_stats.py
- okex
  - okex_indicators_api.py
  - subscribe_longShortRatios.py
- influxDB
  - influxdb_client.py
  - bitmex_influxdb_client.py
  - ftx_influxdb_client.py
  - okex_influxdb_client.py
- checkdb
  - ftx_checkdb.py

### Work in progress
- bitfinex
  - liquidations
  - api_snapshot: https://api-pub.bitfinex.com/v2/liquidations/hist
- okex indicators
  - Basis
  - Open Interest and Trading Volume
  - Taker Buy and Sell
  - Top Trader Sentimental Index
  - Top Trader Average Margin Used


