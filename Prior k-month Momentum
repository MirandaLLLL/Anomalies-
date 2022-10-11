###### 5d 收益率  负相关，取负数
import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close from stock_daily'
data_use=pd.read_sql(sql,conn)


data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])

return_table=data_use[['ts_code','trade_date','close']]
return_table_1=return_table.set_index(["trade_date","ts_code"])
return_table_2=return_table_1['close'].unstack()
return_table_3=(return_table_2-return_table_2.shift())/return_table_2.shift()
return_table_4=return_table_3.stack().reset_index()
return_table_4=return_table_4.rename(columns={0:'return'})
return_table_4=return_table_4.sort_values(by=['ts_code','trade_date'])
return_table_4=return_table_4.set_index(["ts_code","trade_date"]).reset_index()

cul_return_1=return_table_4.set_index(["trade_date","ts_code"])
cul_return_2=cul_return_1['return'].unstack()
cul_return_3=cul_return_2.rolling(5).sum()
cul_return_5=cul_return_3.stack().reset_index()
cul_return_5=cul_return_5.rename(columns={0:'cul_return_5'})
cul_return_5=cul_return_5.sort_values(by=['ts_code','trade_date'])
cul_return_5=cul_return_5.set_index(["ts_code","trade_date"]).reset_index()

comb_1=pd.merge(cul_return_5,data_use,on=['trade_date','ts_code'])
comb_1["index"]=-comb_1["cul_return_5"]
comb_1=comb_1.sort_values(by=['ts_code','trade_date'])
comb_1=comb_1.set_index(["ts_code","trade_date"]).reset_index()
result=comb_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


###### 22d 收益率  负相关，取负数

cul_30_return_1=return_table_4.set_index(["trade_date","ts_code"])
cul_30_return_2=cul_30_return_1.unstack()
cul_30_return_3=cul_30_return_2.rolling(22).sum()
cul_30_return_5=cul_30_return_3.stack().reset_index()
cul_30_return_5=cul_30_return_5.rename(columns={'return':'cul_return_22'})
cul_30_return_5=cul_30_return_5.sort_values(by=['ts_code','trade_date'])
cul_30_return_5=cul_30_return_5.set_index(["ts_code","trade_date"]).reset_index()

comb_1=pd.merge(cul_30_return_5,data_use,on=['trade_date','ts_code'])
comb_1["index"]=-comb_1["cul_return_22"]
comb_1=comb_1.sort_values(by=['ts_code','trade_date'])
comb_1=comb_1.set_index(["ts_code","trade_date"]).reset_index()

result=comb_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

###### 60d 收益率  负相关，取负数

cul_60_return_1=return_table_4.set_index(["trade_date","ts_code"])
cul_60_return_2=cul_60_return_1.unstack()
cul_60_return_3=cul_60_return_2.rolling(60).sum()


cul_60_return_5=cul_60_return_3.stack().reset_index()
cul_60_return_5=cul_60_return_5.rename(columns={'return':'cul_return_60'})
cul_60_return_5=cul_60_return_5.sort_values(by=['ts_code','trade_date'])
cul_60_return_5=cul_60_return_5.set_index(["ts_code","trade_date"]).reset_index()
comb_1=pd.merge(cul_60_return_5,data_use,on=['trade_date','ts_code'])
comb_1["index"]=-comb_1["cul_return_60"]
comb_1=comb_1.sort_values(by=['ts_code','trade_date'])
comb_1=comb_1.set_index(["ts_code","trade_date"]).reset_index()

result=comb_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

###### 120d 收益率  负相关，取负数

cul_return_3=cul_return_2.rolling(120).sum()
cul_return_5=cul_return_3.stack().reset_index()
cul_return_5=cul_return_5.rename(columns={0:'cul_return_5'})
cul_return_5=cul_return_5.sort_values(by=['ts_code','trade_date'])
cul_return_5=cul_return_5.set_index(["ts_code","trade_date"]).reset_index()

comb_1=pd.merge(cul_return_5,data_use,on=['trade_date','ts_code'])
comb_1["index"]=-comb_1["cul_return_5"]
comb_1=comb_1.sort_values(by=['ts_code','trade_date'])
comb_1=comb_1.set_index(["ts_code","trade_date"]).reset_index()
result=comb_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

###### 250d 收益率  负相关，取负数

cul_return_3=cul_return_2.rolling(250).sum()
cul_return_5=cul_return_3.stack().reset_index()
cul_return_5=cul_return_5.rename(columns={0:'cul_return_5'})
cul_return_5=cul_return_5.sort_values(by=['ts_code','trade_date'])
cul_return_5=cul_return_5.set_index(["ts_code","trade_date"]).reset_index()

comb_1=pd.merge(cul_return_5,data_use,on=['trade_date','ts_code'])
comb_1["index"]=-comb_1["cul_return_5"]
comb_1=comb_1.sort_values(by=['ts_code','trade_date'])
comb_1=comb_1.set_index(["ts_code","trade_date"]).reset_index()
result=comb_1[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
