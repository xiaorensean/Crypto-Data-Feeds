import traceback
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
pkg_dir = os.path.dirname(current_dir)
sys.path.append(pkg_dir)
from influxdb_client.bitmex_influxdb_client_v1 import BitmexInfluxClient
from utility.error_logger_writer import logger
import requests 
import json
import pandas as pd
from datetime import datetime
from time import sleep
import numpy as np
import multiprocessing as mp


bitmex = BitmexInfluxClient()


# write all local data files into influxdb
def local_data_fetch(path,options,db_name,measurement):  
    files_temp =sorted([f for f in os.listdir(path) if f != '.DS_Store' and 'raw' in f])
    files = files_temp[-2:] + files_temp[:len(files_temp)-2]
    files_df = []
    files_dict = []
    for f in files:
        file_df = pd.read_excel(path+f,sheet_name=options)
        file_df['CREATION_TIME'] = [str(ts.date())for ts in file_df['CREATION_TIME'].tolist()]
        files_dict.append([file_df.T.to_dict()[idx] for idx in file_df.T.to_dict().keys()])
        files_df.append(file_df)
    df = pd.concat(files_df,axis=0,sort=True)
    influxdb = BitmexInfluxClient()
    time = False
    tags = {}
    for idx in range(len(files_dict)):       
        for jdx in range(len(files_dict[idx])):
            fields = files_dict[idx][jdx]
            influxdb.write_points_to_measurement(db_name,measurement,time,tags,fields)
    return(df)



def collector_notional():
    measurement = 'bitmex_leaderboard_notional'
    bitmex.subscribe_bitmex_leaderboard('notional',measurement)
    while True:
        # scheduler per hours
        sleep(60*60)
        try:
            bitmex.subscribe_bitmex_leaderboard('notional',measurement)
        except Exception:
            error_time = datetime.utcnow()
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            # write to log file
            with open("error_log.txt", "a") as text_file:
                print("Error message for {} at {} \n".format(measurement,error_time) + error_message, file=text_file)
            pass


def collector_roe():
    measurement = 'bitmex_leaderboard_ROE'
    bitmex.subscribe_bitmex_leaderboard('ROE',measurement)
    while True:
        # scheduler per hours
        sleep(60*60)
        try:
            bitmex.subscribe_bitmex_leaderboard('ROE',measurement)
        except Exception:
            error_time = datetime.utcnow()
            error_message = traceback.format_exc()
            #logger(measurement,error_message)
            # write to log file
            with open("error_log.txt", "a") as text_file:
                print("Error message for {} at {} \n".format(measurement,error_time) + error_message, file=text_file)
            pass


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
        excel_name = 'leaderboard_{}.xlsx'.format(str(datetime.utcnow()))
    else:
        leaderboard = parser_data('notional','raw')
        roe = parser_data('ROE','raw')
        excel_name = 'leaderboard_raw{}.xlsx'.format(str(datetime.utcnow()))
    ### to spreadsheet
    all_results = {'notional':leaderboard,'ROE':roe}
    directory = "../test/"
    if not os.path.exists(directory):
        os.makedirs(directory)
    leaderboard.to_excel(directory + excel_name,sheet_name = 'Notional',index=False)
    with pd.ExcelWriter(directory + excel_name) as writer:
        for name in all_results.keys():
            all_results[name].to_excel(writer, sheet_name=name,index=False)


   


if __name__ == '__main__':
    notional_collector = mp.Process(name='notional_collector',target=collector_notional).start()
    roe_collector = mp.Process(name='roe_collector',target=collector_roe).start()




   
