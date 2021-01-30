import requests
from lxml.html import fromstring
from datetime import datetime
import json
import os 
import sys


#url = "https://www.investing.com/common/modules/js_instrument_chart/api/data.php?pair_id=8839&pair_id_for_news=8839&chart_type=area&pair_interval=300&candle_count=120&events=yes&volume_series=yes&period=1-day"
#url = "https://www.investing.com/common/modules/js_instrument_chart/api/data.php"xw
def get_EC_futures(start_date,end_date):
    url = "https://www.investing.com/instruments/HistoricalDataAjax"
    hdr = {
        "User-Agent": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        }

    params = {"curr_id":8839,
          "smllD":500066,
          "st_date":start_date,
          "end_date":end_date,
          "interval_sec":"Daily",
          "sort_col":"date",
          "sort_ord":"DESC"}
    req = requests.post(url,headers=hdr,data=params)
    if req.status_code != 200:
        raise ConnectionError("ERR#0015: error " + str(req.status_code) + ", try again later.")

    root_ = fromstring(req.text)
    path_ = root_.xpath(".//table[@id='curr_table']/tbody/tr")
         
    result = list()

    if path_:
        for elements_ in path_:
            info = []        
            for nested_ in elements_.xpath(".//td"):
                info.append(nested_.get('data-real-value'))
            if info[0] is not None:
                info_dict = {}
                fund_date = datetime.strptime(str(datetime.utcfromtimestamp(int(info[0])).date()), '%Y-%m-%d')                    
                fund_close = float(info[1].replace(',', ''))
                fund_open = float(info[2].replace(',', ''))
                fund_high = float(info[3].replace(',', ''))
                fund_low = float(info[4].replace(',', ''))
                volume = float(info[5].replace(',', ''))
                info_dict.update({"Date":fund_date}) 
                info_dict.update({"Close":fund_close})
                info_dict.update({"Open":fund_open})
                info_dict.update({"High":fund_high})
                info_dict.update({"Low":fund_low})
                info_dict.update({"Volume":volume})
                result.append(info_dict)
            else:
                print("API return empty")
    else:
        print("error")
    
    return result

if __name__ == "__main__":
    data = get_EC_futures("04/01/2020","04/02/2020")