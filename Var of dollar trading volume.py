####成交量 volume  22d，取倒数 

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,vol,amount from stock_daily'
vol_amt_table=pd.read_sql(sql,conn)

sql='select ts_code,trade_date,close,turnover_rate,turnover_rate_f from stock_basic'
turnover_table=pd.read_sql(sql,conn)

vol_amt_table["trade_date"] = pd.to_datetime(vol_amt_table["trade_date"])
data_use_1=vol_amt_table.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['vol'].unstack()

data_use_3=data_use_2.rolling(22).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'22d_vol'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["22d_vol"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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

####成交量 volume  5d，取倒数

data_use_3=data_use_2.rolling(5).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'5d_vol'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["5d_vol"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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

####成交量 volume  60d，取倒数

data_use_3=data_use_2.rolling(60).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'60d_vol'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["60d_vol"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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


####成交量 volume  120d，取倒数

data_use_3=data_use_2.rolling(120).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'120d_vol'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["120d_vol"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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



####成交量 volume  250d，取倒数

data_use_3=data_use_2.rolling(250).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'250d_vol'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["250d_vol"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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


###成交额 amount  22d

data_use_2=data_use_0['amount'].unstack()

data_use_3=data_use_2.rolling(22).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'22d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["22d_amt"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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



###成交额 amount  5d

data_use_3=data_use_2.rolling(5).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'5d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["5d_amt"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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


###成交额 amount  60d

data_use_3=data_use_2.rolling(60).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'60d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["60d_amt"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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


###成交额 amount  120d

data_use_3=data_use_2.rolling(120).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'120d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["120d_amt"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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



###成交额 amount 250d

data_use_3=data_use_2.rolling(250).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'250d_amt'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["250d_amt"]
data_merge=pd.merge(data_use_4,vol_amt_table,on=['trade_date','ts_code'])
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



###所有股换手率 turnover  22d

turnover_table["trade_date"] = pd.to_datetime(turnover_table["trade_date"])
data_use_1=turnover_table.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['turnover_rate'].unstack()

data_use_3=data_use_2.rolling(22).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'22d_tov'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["22d_tov"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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



###所有股换手率 turnover  5d

data_use_3=data_use_2.rolling(5).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'5d_tov'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["5d_tov"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###所有股换手率 turnover  60d

data_use_3=data_use_2.rolling(60).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'60d_tov'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["60d_tov"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###所有股换手率 turnover  120d

data_use_3=data_use_2.rolling(120).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'120d_tov'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["120d_tov"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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



###所有股换手率 turnover 250d

data_use_3=data_use_2.rolling(250).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'250d_tov'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["250d_tov"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###流通股换手率 turnover_f  22d

data_use_2=data_use_0['turnover_rate_f'].unstack()

data_use_3=data_use_2.rolling(22).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'22d_tov_f'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["22d_tov_f"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###流通股换手率 turnover_f  5d

data_use_3=data_use_2.rolling(5).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'5d_tov_f'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["5d_tov_f"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###流通股换手率 turnover_f  60d

data_use_3=data_use_2.rolling(60).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'60d_tov_f'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["60d_tov_f"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###流通股换手率 turnover_f  120d

data_use_3=data_use_2.rolling(120).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'120d_tov_f'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["120d_tov_f"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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


###流通股换手率 turnover_f 250d

data_use_3=data_use_2.rolling(250).std()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'250d_tov_f'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_4["index"]=1/data_use_4["250d_tov_f"]
data_merge=pd.merge(data_use_4,turnover_table,on=['trade_date','ts_code'])
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
