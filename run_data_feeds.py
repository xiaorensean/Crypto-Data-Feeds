'''
Main script
Spawns individual scripts that collect data from exchanges
Constantly checks that all the scripts are running, if any script stops for whatever reason, restart it
'''


import os
from time import sleep
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)))

import multiprocessing

from utility import resubscribe

process_logger = "logger/market_data_host_1_processes.txt"

processes = []

scripts = [
    "aave/subscribe_rates.py",
    "aave/subscribe_coin_reserve.py",
    "aave/subscribe_coin_summary.py",
    "balancer/subscribe_balpool_scrape.py",
    "balancer/subscribe_balpool_summary.py",
    "bigone_exchange/subscribe_trades.py",
    "bigone_exchange/subscribe_orderbook.py",
    "binance/subscribe_borrow_rates.py",
    "binance/subscribe_funding_rates.py",
    "binance/subscribe_open_interest.py",
    "binance/subscribe_trade_futures.py",
    "binance/subscribe_trade_spot.py",
    "binance/subscribe_orderbook_futures.py",
    "binance/subscribe_orderbook_spot.py",
    "binance/subscribe_liquidation_trades.py",
    "binance/subscribe_insurance_fund.py",
    "binance/future_stats_open_interest.py",
    "binance/future_stats_long_short_ratio.py",
    "binance/future_stats_long_short_ratio_account.py",
    "binance/future_stats_long_short_ratio_position.py",
    "binance/future_stats_taker_buy_sell_ratio.py",
    "binance/subscribe_markprice.py",
    "binance/subscribe_mining.py",
    "binance/subscribe_leaderboard.py",
    "bitfinex/subscribe_funding_orderbook.py",
    "bitfinex/subscribe_funding_trades.py",
    "bitfinex/subscribe_leaderboard_3h.py",
    "bitfinex/subscribe_leaderboard_1w.py",
    "bitfinex/subscribe_leaderboard_1m.py",
    "bitmex_exchange/subscribe_funding_rates.py",
    "bitmex_exchange/subscribe_leaderboard.py",
    "bitmex_exchange/subscribe_leaderboard_1m.py",
    "bitmex_exchange/subscribe_slow_metadata.py",
    "bitmex_exchange/subscribe_trades_hist.py",
    "bitmex_exchange/subscribe_trades_ws.py",
    "bitmex_exchange/subscribe_orderbook.py",
    "bybit/subscribe_funding_rates.py",
    "bybit/subscribe_data.py",
    "bybit/subscribe_trades.py",
    "bybit/subscribe_trades_usdt.py",
    "bybit/subscribe_liquidation.py",
    #"coinflex/subscribe_burn_fees.py",
    "cftc/subscribe_cftc_fut.py",
    "compound/subscribe_borrow_rates.py",
    "compound/subscribe_leaderboard.py",
    "compound/subscribe_governance.py",
    "compound/subscribe_market_overview.py",
    "coinbase/subscribe_orderbook.py",
    "coinbase/subscribe_trades.py",
    "coinbase/subscribe_custody.py",
    "coinex/subscribe_trades.py",
    "coinex/subscribe_orderbook.py",
    "cosmoscan/subscribe_cosmos_validators.py",
    "cme_group/subscribe_futures_index.py",
    "curve_fi/subscribe_apy.py",
    "curve_fi/subscribe_data.py",
    "deribit/subscribe_funding_rate.py",
    "deribit/subscribe_orderbook_eth1.py",
    "deribit/subscribe_orderbook_eth2.py",
    "deribit/subscribe_orderbook_eth3.py",
    "deribit/subscribe_orderbook_eth4.py",
    "deribit/subscribe_orderbook_eth5.py",
    "deribit/subscribe_orderbook_eth6.py",
    "deribit/subscribe_orderbook_eth7.py",
    "deribit/subscribe_orderbook_eth8.py",
    "deribit/subscribe_orderbook_btc1.py",
    "deribit/subscribe_orderbook_btc2.py",
    "deribit/subscribe_orderbook_btc3.py",
    "deribit/subscribe_orderbook_btc4.py",
    "deribit/subscribe_orderbook_btc5.py",
    "deribit/subscribe_orderbook_btc6.py",
    "deribit/subscribe_orderbook_btc7.py",
    "deribit/subscribe_orderbook_btc8.py",
    "deribit/subscribe_trades_btc.py",
    "deribit/subscribe_trades_eth.py",
    "dydx/subscribe_borrow_rates.py",
    "ftx/subscribe_funding_rates.py",
    "ftx/subscribe_future_stats.py",
    "ftx/subscribe_trades.py",
    "ftx/subscribe_trades_options.py",
    "ftx/subscribe_option_request.py",
    "ftx/subscribe_orderbook.py",
	"ftx/subscribe_leaderboard.py",
    "ftx/subscribe_leaderboard_1m.py",
    "huobi/subscribe_open_interest.py",
    "huobi/subscribe_index_price.py",
    "huobi/subscribe_data.py",
    "huobi/subscribe_elite_ratio.py",
    "huobi/subscribe_liquidation_futures.py",
    "huobi/subscribe_liquidation_swaps.py",
    "huobi/subscribe_trades_swaps.py",
    "huobi/subscribe_trades_futures.py",
    "huobi/subscribe_orderbook_futures.py",
    "huobi/subscribe_orderbook_swaps.py",
    "huobi/subscribe_insurance_fund_futures.py",
    "huobi/subscribe_insurance_fund_swaps.py",
    "hashpool/subscribe_hashpool_coins.py",
    "investing_data/subscribe_ES_futures.py",
    "gateio/subscribe_orderbook.py",
    "gateio/subscribe_trades.py",
    "mxc_exchange/subscribe_orderbook.py",
    "mxc_exchange/subscribe_trades.py",
    "namebase/subscribe_domain_data.py",
    "namebase/subscribe_orderbook_rest.py",
    "namebase/subscribe_trades.py",
    "okex/subscribe_data.py",
    "okex/subscribe_future_stats_longShortRatios.py",
    "okex/subscribe_future_stats_contractBasis.py",
    "okex/subscribe_future_stats_contract_stats.py",
    "okex/subscribe_future_taker_buy_sell.py",
    "okex/subscribe_future_stats_topTraderSentimentIndex.py",
    "okex/subscribe_future_stats_top_trader_average_margin.py",
    "okex/subscribe_liquidation.py",
    "okex/subscribe_oi_swaps.py",
    "okex/subscribe_option_stats_data.py",
    "okex/subscribe_option_stats_implied_vol.py",
    "okex/subscribe_option_stats_implied_vol_atm.py",
    "okex/subscribe_option_stats_implied_vol_skew.py",
    "okex/subscribe_option_stats_impVol_HistVol.py",
    "okex/subscribe_orderbook_futures_usd1.py",
    "okex/subscribe_orderbook_futures_usd2.py",
    "okex/subscribe_orderbook_futures_usdt1.py",
    "okex/subscribe_orderbook_futures_usdt2.py",
    "okex/subscribe_orderbook_spot.py",
    "okex/subscribe_orderbook_swaps_usd.py",
    "okex/subscribe_orderbook_swaps_usdt.py",
    "okex/subscribe_ticker_swaps.py",
    "okex/subscribe_ticker_futures.py",
    "okex/subscribe_trades_futures_usd.py",
    "okex/subscribe_trades_futures_usdt.py",
    "okex/subscribe_trades_spot.py",
    "okex/subscribe_trades_swaps_usd.py",
    "okex/subscribe_trades_swaps_usdt.py",
    "poloniex/subscribe_funding_orderbook.py",
    "poloniex/subscribe_orderbook.py",
    "poloniex/subscribe_trades.py",
    "poloniex/subscribe_poloniex_leaderboard.py",
    "synthetix/subscribe_open_interest.py",
    "synthetix/subscribe_dashboard_data.py",
    "blockchain/nervos_block.py",
    "blockchain/nervos_hashrate.py",
    "hnscan/hnscan_data.py",
    #"hnscan/hnscan_summary_status.py",
    "hnscan/hnscan_block_info.py",
    "hnscan/hnscan_transaction.py",
    "blockchain/tether_richlist.py",
    "blockchain/cosmos_validator.py",
    "blockchain/subscribe_tezos_leaderboard.py",
    "wazirX/subscribe_volume.py",
]

def create_process(directory, scriptname):
	process = multiprocessing.Process(target=resubscribe.run_forever, args=(directory, scriptname))
	process.start()

	with open(process_logger, "a+") as f:
		lines = ['----------\n', directory + "/" + scriptname + "\n", str(process.pid) + "\n", '----------\n']
		f.writelines(lines)
	entry = {
		"directory": directory,
		"scriptname": scriptname,
		"process": process
	}
	return entry

if __name__ == "__main__":
	open(process_logger, 'w').close()
	restart = False

	for script in scripts:
		directory = script.split("/")[0]
		scriptname = script.split("/")[1]
		processes.append(create_process(directory, scriptname))

	while True:

		for entry in processes:
			process = entry["process"]
			directory = entry["directory"]
			scriptname = entry["scriptname"]
			if not process.is_alive():
				entry["process"].join()
				processes.remove(entry)
				processes.append(create_process(directory, scriptname))
				print(directory + "/" + scriptname )
		sleep(1)
   



