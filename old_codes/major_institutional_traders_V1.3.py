'''
三大法人交易情形統計資料自2012年5月1日起，提供交易日前三年之資料供查詢
，若需歷史資料，請填寫公開資料申請表申請  (只能查前三年)
三大法人交易情形統計資料自2011年12月19日起，揭露之交易資訊含鉅額交易量

from 2015/9/25-2018/9/25
'''

import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
from tabulate import tabulate
import datetime
from dateutil.rrule import rrule, DAILY
import time
import gc
import sqlite3


def get_data(start_date, end_date, target, sleep_sec = 2):
    s = time.time()
    start_date  = datetime.datetime.strptime(start_date, '%Y/%m/%d')
    end_date  = datetime.datetime.strptime(end_date, '%Y/%m/%d')
    date_list = []

    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        date_list.append(dt.date())

    url = "https://www.taifex.com.tw/chinese/3/7_12_3.asp"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
    }

    indexs = ['日期','身分',
             '多方交易口數','多方契約金額',
             '空方交易口數','空方契約金額',
             '多空淨額口數','多空淨額契約金額',
             '未平倉多方交易口數','未平倉多方契約金額',
             '未平倉空方交易口數','未平倉空方契約金額',
             '未平倉多空淨額口數','未平倉多空淨額契約金額']
    df = pd.DataFrame(columns=indexs)


    for d in date_list:
        Y = str(d.timetuple().tm_year)
        M = str(d.timetuple().tm_mon)
        D = str(d.timetuple().tm_mday)
        datestart = "{}/{}/{}".format(Y, M, D)

        post_data={
            'goday':'', 
            'DATA_DATE_Y': Y,
            'DATA_DATE_M': M,
            'DATA_DATE_D': D,
            'syear': Y,
            'smonth': M,
            'sday': D,
            'datestart': datestart,     
            'COMMODITY_ID': target}    #這部分是網頁契約，空字串''表示全部，TXF表示臺股期貨，EXF表示電子期貨.....

        time.sleep(sleep_sec)

        response = requests.post(url,headers=headers,data=post_data)     # 這個網站是透過post要求資料的
        response.encoding = "utf-8"

        soup = BeautifulSoup(response.text, "lxml")     # 網頁html
        rows = soup.select('table.table_f tr')    # 搜尋期貨契約表格下的一列列tr資料，table_f 是目標table的class name, 以tr為單位存進list
        if rows == []:    # 跳過未開市日期
            pass
        else:
            for row in range(3,6):
                # 每個商品名稱都有三個身分別，自營商、投信、外資，一個商品有三個tr，每個商品第二及第三個tr都比第一個tr少兩個td
                if row == 3:
                    data_day = [rows[row].contents[i*2-1].get_text().replace(',',"").rstrip() for i in range(3,16)] 
                else:
                    data_day = [rows[row].contents[i*2-1].get_text().replace(',',"").rstrip() for i in range(1,14)]
                data_day.insert(0,d)
                df = df.append(pd.Series(data_day, index = indexs), ignore_index=True)

    print('get_data cost time:', time.time()-s)
    return df

if __name__ == '__main__':
    s = time.time()
    df = get_data('2018/01/01', '2018/01/03', 'TXF', 2)
    
    save_path = "C:/Users/ASUS/Desktop/JupyterWorkplace/major_institutional_traders/"
    with sqlite3.connect("{}test1.db".format(save_path)) as conn:
        df.to_sql(name = 'major_institutional_traders', con = conn, if_exists = 'replace')

    df.to_csv("{}major_institutional_traders.csv".format(save_path))

    del(df); gc.collect()
    print('total cost time:', time.time()-s)
