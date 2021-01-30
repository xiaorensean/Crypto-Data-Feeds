import traceback
import time
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
from metadata_leaderboard import write_weekly_data
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from utility.error_logger_writer import logger

measurement = "bitfinex_leaderboard"

if __name__ == "__main__":
    try:
        write_weekly_data(measurement)
    except Exception:
        error = traceback.format_exc()
        #logger(measurement, error)
    while True:
        time.sleep(60*60*24*7)
        try:
            write_weekly_data(measurement)
        except Exception:
            error = traceback.format_exc()
            #logger(measurement, error)


