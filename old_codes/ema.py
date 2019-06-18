import numpy as np 
import pandas as pd
from datetime import datetime

def ema(data, alpha = 0.2, period = 5):
    df = pd.DataFrame(data)
    df['ema'] = 0.0
    df.ema[period-1] = df.price[:period].mean() 

    for i in range(period,len(df)):
        df.ema[i] = alpha*df.price[i]+(1-alpha)*df.ema[i-1]
    
    return(df[['date','ema']][period-1:])



if __name__ == '__main__':
    # test data
    P1 = [{"date":datetime.strptime('2018-01-01','%Y-%m-%d'),'price':11},
		 {"date":datetime.strptime('2018-01-02','%Y-%m-%d'),'price':12}, 
		 {"date":datetime.strptime('2018-01-03','%Y-%m-%d'),'price':13}, 
		 {"date":datetime.strptime('2018-01-04','%Y-%m-%d'),'price':14}, 
		 {"date":datetime.strptime('2018-01-05','%Y-%m-%d'),'price':15},
		 {"date":datetime.strptime('2018-01-06','%Y-%m-%d'),'price':16},
		 {"date":datetime.strptime('2018-01-07','%Y-%m-%d'),'price':17},
		 {"date":datetime.strptime('2018-01-08','%Y-%m-%d'),'price':18},
		 {"date":datetime.strptime('2018-01-09','%Y-%m-%d'),'price':19},
		 {"date":datetime.strptime('2018-01-10','%Y-%m-%d'),'price':20}]
		 	 
    EMA = ema(data = P1, alpha = 0.2, period = 5)
    print(EMA)