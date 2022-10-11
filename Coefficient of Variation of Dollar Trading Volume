####成交量 volume 22d 取倒数

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,vol,amount from stock_daily'
vol_table=pd.read_sql(sql,conn)

data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
data_use_1=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['vol'].unstack()

data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_22d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(22).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'vol_22d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["vol_22d_cvd"]=data_merge_1["vol_22d_std"]/data_merge_1["vol_22d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["vol_22d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####成交量 volume 5d 取倒数

data_use_3=data_use_2.rolling(5).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_5d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(5).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'vol_5d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["vol_5d_cvd"]=data_merge_1["vol_5d_std"]/data_merge_1["vol_5d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["vol_5d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()



factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交量 volume 60d 取倒数

data_use_3=data_use_2.rolling(60).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_60d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(60).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'vol_60d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["vol_60d_cvd"]=data_merge_1["vol_60d_std"]/data_merge_1["vol_60d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["vol_60d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交量 volume 120d 取倒数

data_use_3=data_use_2.rolling(120).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_120d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(120).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'vol_120d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["vol_120d_cvd"]=data_merge_1["vol_120d_std"]/data_merge_1["vol_120d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["vol_120d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()



factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交量 volume 250d 取倒数

data_use_3=data_use_2.rolling(250).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'vol_250d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(250).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'vol_250d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["vol_250d_cvd"]=data_merge_1["vol_250d_std"]/data_merge_1["vol_250d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["vol_250d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()



factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额 amount 22d 取倒数

data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
data_use_1=data_use.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1=data_use_1.set_index(["trade_date","ts_code"]).reset_index()
data_use_0=data_use_1.set_index(["trade_date","ts_code"])
data_use_2=data_use_0['amount'].unstack()

data_use_3=data_use_2.rolling(22).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_22d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(22).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'amt_22d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["amt_22d_cvd"]=data_merge_1["amt_22d_std"]/data_merge_1["amt_22d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["amt_22d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额 amount 5d 取倒数

data_use_3=data_use_2.rolling(5).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_5d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(5).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'amt_5d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["amt_5d_cvd"]=data_merge_1["amt_5d_std"]/data_merge_1["amt_5d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["amt_5d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额 amount 60d 取倒数

data_use_3=data_use_2.rolling(60).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_60d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(60).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'amt_60d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["amt_60d_cvd"]=data_merge_1["amt_60d_std"]/data_merge_1["amt_60d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["amt_60d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额 amount 120d 取倒数

data_use_3=data_use_2.rolling(120).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_120d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(120).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'amt_120d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["amt_120d_cvd"]=data_merge_1["amt_120d_std"]/data_merge_1["amt_120d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["amt_120d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####成交额 amount 250d 取倒数

data_use_3=data_use_2.rolling(250).mean()
data_use_4=data_use_3.stack().reset_index()

data_use_4=data_use_4.rename(columns={0:'amt_250d_mean'})
data_use_4=data_use_4.sort_values(by=['ts_code','trade_date'])
data_use_4=data_use_4.set_index(["ts_code","trade_date"]).reset_index()

data_use_5=data_use_2.rolling(250).std()
data_use_6=data_use_5.stack().reset_index()

data_use_6=data_use_6.rename(columns={0:'amt_250d_std'})
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["ts_code","trade_date"]).reset_index()

data_merge_1=pd.merge(data_use_6,data_use_4,on=['ts_code','trade_date'])
data_merge_1["amt_250d_cvd"]=data_merge_1["amt_250d_std"]/data_merge_1["amt_250d_mean"]
data_merge_2=pd.merge(data_merge_1,data_use_1,on=['trade_date','ts_code'])
data_merge_2=data_merge_2.sort_values(by=['ts_code','trade_date'])
data_merge_2=data_merge_2.set_index(["trade_date","ts_code"]).reset_index()
data_merge_2["index"]=1/data_merge_2["amt_250d_cvd"]

result=data_merge_2[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
