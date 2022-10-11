###turnover_rate 与 收益成负相关，取1/turnover_rate
import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,turnover_rate,turnover_rate_f from stock_basic'
Turnover_table=pd.read_sql(sql,conn)

data_use=Turnover_table.set_index(["trade_date","ts_code"]).reset_index()
data_use=data_use.sort_values(by=['ts_code','trade_date'])
data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])

data_use["turnover_rate"]=1/data_use["turnover_rate"]
result=data_use[['ts_code','trade_date','close','turnover_rate']]
final=result.set_index(["trade_date","ts_code"])

factor_init = final["turnover_rate"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)



###换手率_自由流通股

data_use=Turnover_table.set_index(["trade_date","ts_code"]).reset_index()
data_use=data_use.sort_values(by=['ts_code','trade_date'])
data_use=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])

data_use["index"]=1/data_use["turnover_rate_f"]
result=data_use[['ts_code','trade_date','close','index']]

final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
