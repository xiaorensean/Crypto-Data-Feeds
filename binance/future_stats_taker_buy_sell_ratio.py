from multiprocessing import Process
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from metadata_long_short_ratio import subscribe_long_short_ratio

measurement = 'binance_future_stats_taker_buy_sell_volume'
lsr_type = "volume"
freqs = [5,15,30,60,120,240,360,720,1440]


if __name__ == "__main__":
    for freq in freqs:
        freq_subscription = Process(target=subscribe_long_short_ratio,args=(freq,measurement,lsr_type))
        freq_subscription.start()
    
