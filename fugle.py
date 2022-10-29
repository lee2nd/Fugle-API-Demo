# -*- coding: utf-8 -*-
"""
Created on Sun Jan 16 23:15:33 2022

@author: frank lee
"""

import requests
import sqlite3
import pandas as pd
from datetime import date
from apscheduler.schedulers.background import BackgroundScheduler
import warnings
warnings.filterwarnings('ignore')

# 連進 sql
conn = sqlite3.connect(r'fugle demo.db', check_same_thread=False) 
    
def add_to_sql():   
    
    payload = {
        "symbolId": 2888,
        "apiToken": "0789086386cfa8c6d6caf4046b107ca2"
    }

    res_quote = requests.get("https://api.fugle.tw/realtime/v0.3/intraday/quote", params=payload)
    json_data_quote = res_quote.json()

    res_meta = requests.get("https://api.fugle.tw/realtime/v0.3/intraday/meta", params=payload)
    json_data_meta = res_meta.json()

    data = {
        "Date": date.today(),
        "open": json_data_quote["data"]["quote"]["priceOpen"]["price"],
        "high": json_data_quote["data"]["quote"]["priceHigh"]["price"],
        "low": json_data_quote["data"]["quote"]["priceLow"]["price"],
        "close": json_data_quote["data"]["quote"]["trade"]["price"],
        "lastClose": json_data_meta["data"]["meta"]["priceReference"]
    }    
    # dict to dataframe
    data = pd.DataFrame([data])
    print(data)
    # 將資料存入sqlite
    data.to_sql('新光金', conn, if_exists='append')

# 排程設定
sched = BackgroundScheduler()
# sched.configure(jobstores=jobstores, timezone=utc)
# 每5秒觸發
sched.add_job(add_to_sql, 'interval', seconds=5)
sched.start()