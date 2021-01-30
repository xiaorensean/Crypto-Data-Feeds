import traceback
import os
import sys
import time
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from Aave_api.AaveWsApi import get_coin_reserve

sys.path.append(os.path.dirname(current_dir))
from influxdb_client.influxdb_client_v1 import InfluxClient
from utility.error_logger_writer import logger

db = InfluxClient()

measurement = "aave_reserve"
def subscribe_reserve(measurement):
    data = get_coin_reserve()
    try:
        data["errors"]
        return
    except:
        for d in data:
            adjustment_liq = 10**15
            adjustment_rate = 10**27
            fields = {}
            fields.update({"avaiable_liquidity":float(d['availableLiquidity'])/adjustment_liq})
            fields.update({'fee_collected':float(d['lifetimeFeeCollected'])/adjustment_liq})
            fields.update({'fee_originated':float(d['lifetimeFeeOriginated'])/adjustment_liq})
            fields.update({'flash_loans':float(d['lifetimeFlashLoans'])/adjustment_liq})
            fields.update({"depositor_fee":float(d['lifetimeFlashloanDepositorsFee'])/adjustment_liq})
            fields.update({"protocol_fees":float(d['lifetimeFlashloanProtocolFee'])/adjustment_liq})
            fields.update({"liquidated":float(d['lifetimeLiquidated'])/adjustment_liq})
            fields.update({"liquidaity_rate":float(d['liquidityRate'])/adjustment_rate})
            fields.update({"borrow_rate_stable":float(d['stableBorrowRate'])/adjustment_rate})
            fields.update({"total_borrow":float(d["totalBorrows"])/adjustment_liq})
            fields.update({"total_borrow_stable":float(d['totalBorrowsStable'])/adjustment_liq})
            fields.update({"total_borrow_variable": float(d['totalBorrowsVariable']) / adjustment_liq})
            fields.update({"total_liquidity":float(d['totalLiquidity'])/adjustment_liq})
            fields.update({"borrow_rate_variable":float(d['variableBorrowRate'])/adjustment_rate})
            fields.update({"is_api_return_timestamp":True})
            tags = {}
            tags.update({"symbol":d["symbol"]})
            dbtime = datetime.datetime.utcfromtimestamp(d['lastUpdateTimestamp'])
            db.write_points_to_measurement(measurement,dbtime,tags,fields)

if __name__ == "__main__":
    subscribe_reserve(measurement)
    while True:
        time.sleep(55)
        subscribe_reserve(measurement)
