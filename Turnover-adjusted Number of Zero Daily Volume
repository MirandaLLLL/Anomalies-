##### x=5d,k=5d  正相关  turnover

import pandas as pd
import numpy as np
import alphalens as al
import pymysql
import math

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,turnover_rate_f,turnover_rate from stock_basic'
Turnover_table=pd.read_sql(sql,conn)
Turnover_table["trade_date"] = pd.to_datetime(Turnover_table["trade_date"])

sql='select ts_code,trade_date,close,vol,amount from stock_daily'
vol_table=pd.read_sql(sql,conn)
vol_table["trade_date"] = pd.to_datetime(vol_table["trade_date"])

data_use=pd.merge(Turnover_table,vol_table,on=['ts_code','trade_date','close'])
data_use=data_use.sort_values(by=['ts_code','trade_date'])
data_use=data_use.set_index(["trade_date","ts_code"]).reset_index()

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(5).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(5).sum()

max_turn_1=sum_turn_2.rolling(5).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=5d,k=22d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(5).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(22).sum()

max_turn_1=sum_turn_2.rolling(22).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=60d  正相关  turnover


data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(5).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(60).sum()

max_turn_1=sum_turn_2.rolling(60).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=120d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(5).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(120).sum()

max_turn_1=sum_turn_2.rolling(120).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=250d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(5).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(250).sum()

max_turn_1=sum_turn_2.rolling(250).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=5d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(22).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(5).sum()

max_turn_1=sum_turn_2.rolling(5).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=22d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(22).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(22).sum()

max_turn_1=sum_turn_2.rolling(22).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=60d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(22).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(60).sum()

max_turn_1=sum_turn_2.rolling(60).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=120d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(22).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(120).sum()

max_turn_1=sum_turn_2.rolling(120).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=250d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(22).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(250).sum()

max_turn_1=sum_turn_2.rolling(250).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=5d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(60).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate'].unstack()
sum_turn_2=1/sum_turn_1.rolling(5).sum()

max_turn_1=sum_turn_2.rolling(5).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=22d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(60).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(22).sum()

max_turn_1=sum_turn_2.rolling(22).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=60d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(60).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(60).sum()

max_turn_1=sum_turn_2.rolling(60).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()



factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=120d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(60).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(120).sum()

max_turn_1=sum_turn_2.rolling(120).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=250d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(60).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(250).sum()

max_turn_1=sum_turn_2.rolling(250).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=5d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(120).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(5).sum()

max_turn_1=sum_turn_2.rolling(5).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=22d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(120).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(22).sum()

max_turn_1=sum_turn_2.rolling(22).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=60d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(120).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(60).sum()

max_turn_1=sum_turn_2.rolling(60).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=120d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(120).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(120).sum()

max_turn_1=sum_turn_2.rolling(120).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=250d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(120).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(250).sum()

max_turn_1=sum_turn_2.rolling(250).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=5d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(250).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(5).sum()

max_turn_1=sum_turn_2.rolling(5).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=22d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(250).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(22).sum()

max_turn_1=sum_turn_2.rolling(22).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=250d,k=60d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(250).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(60).sum()

max_turn_1=sum_turn_2.rolling(60).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=120d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(250).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(120).sum()

max_turn_1=sum_turn_2.rolling(120).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=250d,k=250d  正相关  turnover

data_use_1=data_use.set_index(["trade_date","ts_code"])
data_use_2=data_use_1['vol'].unstack()
data_use_3=data_use_2
condi=data_use_3<15000
data_use_3=data_use_3.where(condi,0)
condi_1=data_use_3==0
data_use_3=data_use_3.where(condi_1,1)

data_use_3=data_use_3.rolling(250).sum()
data_use_4=data_use_3.unstack().reset_index()
data_use_4=data_use_4.rename(columns={0:'nvd'})
sum_turn_1=data_use_1['turnover_rate_f'].unstack()
sum_turn_2=1/sum_turn_1.rolling(250).sum()

max_turn_1=sum_turn_2.rolling(250).max()
max_turn_2=max_turn_1+1
####max_turn_2=max_turn_1.apply(np.floor)

max_turn_3=max_turn_2.unstack().reset_index()
max_turn_3=max_turn_3.rename(columns={0:'max'})
sum_turn_3=sum_turn_2.unstack().reset_index()
sum_turn_3=sum_turn_3.rename(columns={0:'sum'})
deflator_1=pd.merge(max_turn_3,sum_turn_3,on=['trade_date','ts_code'])
deflator_1["deflaror"]=deflator_1["sum"]/deflator_1["max"]

merge_1=pd.merge(deflator_1,data_use_4,on =['trade_date','ts_code'])
merge_2=pd.merge(data_use,merge_1,on =['trade_date','ts_code'])

merge_2=merge_2.sort_values(by=['ts_code','trade_date'])
merge_2=merge_2.set_index(["trade_date","ts_code"]).reset_index()
merge_2["Lm"]=merge_2["deflaror"]+merge_2["nvd"]

merge_2["index"]=merge_2["Lm"]

result=merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=5d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=5d,k=22d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=60d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=5d,k=120d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=5d,k=250d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=5d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=22d  正相关  turnover_f


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=60d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=120d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=22d,k=250d  正相关  turnover_f


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=5d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=22d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=60d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=120d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=60d,k=250d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=5d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=22d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=60d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=120d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=120d,k=250d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


##### x=250d,k=5d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=22d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=60d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=120d  正相关  turnover_f

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

##### x=250d,k=250d  正相关  turnover_f


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)




