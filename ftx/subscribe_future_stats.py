import traceback
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.ftx_influxdb_client import FTXInfluxDBClient
from utility.error_logger_writer import logger
from time import sleep


ftx = FTXInfluxDBClient()


def ftx_future_stats():
    try:
        ftx.subscribe_future_stats()
    except Exception:
        error_message = traceback.format_exc()
        logger("FTX_future_stats",error_message)
    while True:
        sleep(60)
        try:
            ftx.subscribe_future_stats()
        except Exception:
            error_message = traceback.format_exc()
            logger("FTX_future_stats",error_message)

            
if __name__ == '__main__':
    ftx_future_stats()
