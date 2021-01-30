import requests 
import json
import pandas as pd
from datetime import datetime
from time import sleep
import numpy as np
import os


# parse data and save to dataframe
def parser_data(options,raw=None):
    url = 'https://www.bitmex.com/api/v1/leaderboard?method={}'.format(options)
    response = requests.get(url)
    data = response.text
    json_format = json.loads(data)
    df = pd.DataFrame(json_format)
    df['Rank'] = range(1,(df.shape[0]+1))
    df['CREATION_TIME'] = [datetime.utcnow()]*df.shape[0] 
    cols = ['Rank','name','profit','isRealName', 'CREATION_TIME']
    if raw is None:
        if options == 'ROE':
            df['profit'] =[str(np.round(p*100,decimals=2))+'%' for p in df['profit'].tolist()]
        if options == 'noxtional':
            df['profit'] = [str(np.round(p/100000000,decimals=2))+' XBT' for p in df['profit'].tolist()]
    if raw is not None:
        pass
    df = df[cols]
    return df


# save data to spreadsheet
def local_save(raw=None):
    if raw is None:
        leaderboard = parser_data('notional',None)
        roe = parser_data('ROE',None)
        #excel_name = 'leaderboard_{}.xlsx'.format(str(datetime.utcnow()))
    else:
        leaderboard = parser_data('notional','raw')
        roe = parser_data('ROE','raw')
        #excel_name = 'leaderboard_raw{}.xlsx'.format(str(datetime.utcnow()))
    ### to spreadsheet
    all_results = {'notional':leaderboard,'ROE':roe}
    directory = "../bitmex_leaderboard_data/"
    if not os.path.exists(directory):
        os.makedirs(directory)
    all_results['notional'].to_csv(directory + 'notional_{}.csv'.format(str(datetime.utcnow())),index=False)
    all_results['ROE'].to_csv(directory + 'roe_{}.csv'.format(str(datetime.utcnow())),index=False)
    #with pd.ExcelWriter(directory + excel_name) as writer:
    #    for name in all_results.keys():
    #        all_results[name].to_excel(writer, sheet_name=name,index=False)

   


if __name__ == '__main__':
    #subscribe_bitmex_data('notional','bitmex_leaderboard_notional')
    #subscribe_bitmex_data('ROE','bitmex_leaderboard_ROE')
    #local_save()
    # raw data format
    local_save('raw')
    while True:
        # scheduler every 12 hours
        sleep(60*60)
        print('updating')
        local_save('raw')
    #    subscribe_bitmex_data('notional','bitmex_leaderboard_notional')
    #    subscribe_bitmex_data('ROE','bitmex_leaderboard_ROE')


   