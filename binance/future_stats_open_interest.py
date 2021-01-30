from multiprocessing import Process
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from metadata_open_interest import subscribe_open_interest

measurement = 'binance_future_stats_open_interest'
freqs = [5,15,30,60,120,240,360,720,1440]


if __name__ == "__main__":
    for freq in freqs:
        freq_subscription = Process(target=subscribe_open_interest,args=(freq,measurement))
        freq_subscription.start()
    


        
