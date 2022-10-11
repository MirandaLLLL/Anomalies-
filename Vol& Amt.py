####成交量   shift_1d，负相关，取倒数

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,vol,amount from stock_daily'
data_use=pd.read_sql(sql,conn)

data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
data_use_1=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['vol'].unstack()

data_use_3=data_use_2.shift()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'shift_1d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["shift_1d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交量   1d，负相关，取倒数

data_merge["index_curr"]=1/data_merge["vol"]
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index_curr']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index_curr"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交量  根据前22天平均交易量的均值

data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_22d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["vol_22d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交量  根据前5天平均交易量的均值

data_use_3=data_use_2.rolling(5).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_5d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["vol_5d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交量  根据前60天平均交易量的均值

data_use_3=data_use_2.rolling(60).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_60d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["vol_60d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交量  根据前120天平均交易量的均值

data_use_3=data_use_2.rolling(120).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_120d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["vol_120d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交量  根据前250天平均交易量的均值

data_use_3=data_use_2.rolling(250).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_250d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["vol_250d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)



####成交额   shift_1d，负相关，取倒数

data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
data_use_1=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['amount'].unstack()

data_use_3=data_use_2.shift()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'shift_1d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["shift_1d_amt"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交额   1d，负相关，取倒数

data_merge["index_curr"]=1/data_merge["amount"]
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index_curr']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index_curr"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额  根据前22天平均交易量的均值

data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_22d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["amt_22d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额  根据前5天平均交易量的均值

data_use_3=data_use_2.rolling(5).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_5d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["amt_5d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额  根据前60天平均交易量的均值

data_use_3=data_use_2.rolling(60).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_60d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["amt_60d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交额  根据前120天平均交易量的均值

data_use_3=data_use_2.rolling(120).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_120d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["amt_120d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交额  根据前250天平均交易量的均值

data_use_3=data_use_2.rolling(250).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_250d'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["amt_250d"]
data_merge=pd.merge(data_use_4,data_use,on=['trade_date','ts_code'])
data_merge=data_merge.sort_values(by=['ts_code','trade_date'])
data_merge=data_merge.set_index(["trade_date","ts_code"]).reset_index()
result=data_merge[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
