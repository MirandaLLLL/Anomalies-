###### 当前22天的turnover 均值/ 200天前 22天的turnover均值   取倒数

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,turnover_rate,turnover_rate_f from stock_basic'

Turnover_table=pd.read_sql(sql,conn)
Turnover_table["trade_date"] = pd.to_datetime(Turnover_table["trade_date"])
data_use=Turnover_table.set_index(["trade_date","ts_code"]).reset_index()
data_use=data_use.sort_values(by=['ts_code','trade_date'])

data_use=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['turnover_rate'].unstack()
data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()
data_use_4=data_use_4.rename(columns={0:'22d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(250).mean()
data_use_6=data_use_5.stack().reset_index()
data_use_6=data_use_6.rename(columns={0:'250d_mean'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()


data_use_7=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_use_7['abturn']=data_use_7['22d_mean']/data_use_7['250d_mean']

data_use_7['index']=1/data_use_7['abturn']
data_merge=pd.merge(data_use_7,data_use,on=['ts_code','trade_date'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["ts_code","trade_date"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### 流通股换手率

data_use=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['turnover_rate_f'].unstack()
data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()
data_use_4=data_use_4.rename(columns={0:'22d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(250).mean()
data_use_6=data_use_5.stack().reset_index()
data_use_6=data_use_6.rename(columns={0:'250d_mean'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_use_7=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_use_7['abturn']=data_use_7['22d_mean']/data_use_7['250d_mean']

data_use_7['index']=1/data_use_7['abturn']
data_merge=pd.merge(data_use_7,data_use,on=['ts_code','trade_date'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["ts_code","trade_date"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
