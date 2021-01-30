import traceback
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.ftx_influxdb_client import FTXInfluxDBClient
from utility.error_logger_writer import logger
from time import sleep


ftx = FTXInfluxDBClient()


def ftx_funding_rates():
    try:
        ftx.subscribe_funding_rates()
    except Exception:
        error_message = traceback.format_exc()
        logger("FTX_funding_rates_error",error_message)
    while True:
        sleep(60)
        try:
            ftx.subscribe_funding_rates()
        except Exception:
            error_message = traceback.format_exc()
            logger("FTX_funding_rates_error",error_message)


if __name__ == '__main__':
    ftx_funding_rates()
    #funding_rates = mp.Process(name='FTX_funding_rates',target=ftx_funding_rates).start()
