####   firm size  （一天平均）取倒数 

import pandas as pd
import numpy as np
import alphalens as al
import pymysql

conn=pymysql.connect(host='10.3.72.130',database='quant',user='root',password='stark12345+',port=3306)
sql='select ts_code,trade_date,close,total_share from stock_basic'
vol_table=pd.read_sql(sql,conn)

data_use=vol_table.set_index(["trade_date","ts_code"]).reset_index()
data_use=data_use.sort_values(by=['ts_code','trade_date'])
data_use["trade_date"] = pd.to_datetime(data_use["trade_date"])
data_use["index"]=data_use["close"]*data_use["total_share"]

data_use["index"]=1/data_use["index"]
result=data_use[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])

factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####   firm size  （取前5天平均）取倒数 

data_use_1=vol_table.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1["trade_date"] = pd.to_datetime(data_use_1["trade_date"])
data_use_1["firm_size"]=data_use_1["close"]*data_use_1["total_share"]
data_use_2=data_use_1.set_index(["trade_date","ts_code"])
data_use_3=data_use_2['firm_size'].unstack()
data_use_4=data_use_3.rolling(5).mean()
data_use_5=data_use_4.stack().reset_index()
data_use_5=data_use_5.rename(columns={0:'5d_firm_size'})
###data_use_5=data_use_5.drop_duplicates(subset=['trade_date','ts_code'],keep='last')
vol_table["trade_date"] = pd.to_datetime(vol_table["trade_date"])
data_use_6=pd.merge(data_use_5,data_use_1,on=['trade_date','ts_code'])
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["trade_date","ts_code"]).reset_index()

data_use_6["index"]=1/data_use_6["5d_firm_size"]
result=data_use_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)


####   firm size  （取前30天平均）取倒数 

data_use_1=vol_table.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1["trade_date"] = pd.to_datetime(data_use_1["trade_date"])
data_use_1["firm_size"]=data_use_1["close"]*data_use_1["total_share"]
data_use_2=data_use_1.set_index(["trade_date","ts_code"])
data_use_3=data_use_2['firm_size'].unstack()
data_use_4=data_use_3.rolling(30).mean()
data_use_5=data_use_4.stack().reset_index()
data_use_5=data_use_5.rename(columns={0:'30d_firm_size'})
###data_use_5=data_use_5.drop_duplicates(subset=['trade_date','ts_code'],keep='last')
vol_table["trade_date"] = pd.to_datetime(vol_table["trade_date"])
data_use_6=pd.merge(data_use_5,data_use_1,on=['trade_date','ts_code'])
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["trade_date","ts_code"]).reset_index()

data_use_6["index"]=1/data_use_6["30d_firm_size"]
result=data_use_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)

####   firm size  （取前60天平均）取倒数 

data_use_1=vol_table.set_index(["trade_date","ts_code"]).reset_index()
data_use_1=data_use_1.sort_values(by=['ts_code','trade_date'])
data_use_1["trade_date"] = pd.to_datetime(data_use_1["trade_date"])
data_use_1["firm_size"]=data_use_1["close"]*data_use_1["total_share"]
data_use_2=data_use_1.set_index(["trade_date","ts_code"])
data_use_3=data_use_2['firm_size'].unstack()
data_use_4=data_use_3.rolling(60).mean()
data_use_5=data_use_4.stack().reset_index()
data_use_5=data_use_5.rename(columns={0:'60d_firm_size'})
###data_use_5=data_use_5.drop_duplicates(subset=['trade_date','ts_code'],keep='last')
vol_table["trade_date"] = pd.to_datetime(vol_table["trade_date"])
data_use_6=pd.merge(data_use_5,data_use_1,on=['trade_date','ts_code'])
data_use_6=data_use_6.sort_values(by=['ts_code','trade_date'])
data_use_6=data_use_6.set_index(["trade_date","ts_code"]).reset_index()

data_use_6["index"]=1/data_use_6["60d_firm_size"]
result=data_use_6[['ts_code','trade_date','close','index']]
final=result.set_index(["trade_date","ts_code"])
factor_init = final["index"].copy()
price_df = final["close"].unstack().copy()

factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,groupby=None,binning_by_group=False,quantiles=5,bins=None,periods=(1,5,10,20),filter_zscore=20,groupby_labels=None,max_loss=1,zero_aware=False,cumulative_returns=True)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_information_tear_sheet(factor, group_neutral=False,by_group=False)
al.tears.create_turnover_tear_sheet(factor, turnover_periods=None)
