####### volume shift_1d 正相关

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,vol,amount from stock_daily'
data_use=pd.read_sql(sql,conn)

data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
return_table=data_use[['ts_code','trade_date','close']]

return_table=return_table.sort_values(by=['ts_code','trade_date'])
return_table=return_table.set_index(["trade_date","ts_code"]).reset_index()
return_table_1=return_table.set_index(["trade_date","ts_code"])
return_table_2=return_table_1['close'].unstack()
return_table_3=abs(return_table_2-return_table_2.shift())/return_table_2.shift()
return_table_4=return_table_3.stack().reset_index()
return_table_4=return_table_4.rename(columns={0:'return'})
return_table_4=return_table_4.sort_values(by=['ts_code','trade_date'])
return_table_4=return_table_4.set_index(["ts_code","trade_date"]).reset_index()

merge_1=pd.merge(return_table_4,data_use,on=['trade_date','ts_code'])
merge_1["Ami"]=merge_1["return"]/merge_1["vol"]
merge_1=merge_1.sort_values(by=['ts_code','trade_date'])
merge_1=merge_1.set_index(["ts_code","trade_date"]).reset_index()
merge_2=merge_1.set_index(["trade_date","ts_code"])
merge_3=merge_2['Ami'].unstack()
merge_4=merge_3.shift()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'shift_1d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["shift_1d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####### volume 1d  正相关

merge_1["index"]=merge_1["Ami"]

result=merge_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### volume 22d  正相关

merge_4=merge_3.rolling(22).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'22d_vol_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["22d_vol_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### volume 5d   正相关

merge_4=merge_3.rolling(5).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'5d_vol_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["5d_vol_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### volume 60d  正相关

merge_4=merge_3.rolling(60).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'60d_vol_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["60d_vol_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####### volume 120d   正相关

merge_4=merge_3.rolling(120).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'120d_vol_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["120d_vol_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####### volume 250d   正相关

merge_4=merge_3.rolling(250).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'250d_vol_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["250d_vol_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount shift_1d   正相关

merge_1=pd.merge(return_table_4,data_use,on=['trade_date','ts_code'])
merge_1["Ami"]=merge_1["return"]/merge_1["amount"]
merge_1=merge_1.sort_values(by=['ts_code','trade_date'])
merge_1=merge_1.set_index(["ts_code","trade_date"]).reset_index()
merge_2=merge_1.set_index(["trade_date","ts_code"])
merge_3=merge_2['Ami'].unstack()
merge_4=merge_3.shift()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'shift_1d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["shift_1d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 1d   正相关

merge_1["index"]=merge_1["Ami"]

result=merge_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 22d  正相关

merge_4=merge_3.rolling(22).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'amt_22d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["amt_22d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 5d   正相关

merge_4=merge_3.rolling(5).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'amt_5d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["amt_5d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 60d

merge_4=merge_3.rolling(60).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'amt_60d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["amt_60d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 120d

merge_4=merge_3.rolling(120).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'amt_120d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["amt_120d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####### amount 250d

merge_4=merge_3.rolling(250).mean()
merge_5=merge_4.stack().reset_index()
merge_5=merge_5.rename(columns={0:'amt_250d_Ami'})
merge_6=pd.merge(merge_5,data_use,on=['trade_date','ts_code'])
merge_6=merge_6.sort_values(by=['ts_code','trade_date'])
merge_6=merge_6.set_index(["ts_code","trade_date"]).reset_index()

merge_6["index"]=merge_6["amt_250d_Ami"]

result=merge_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
