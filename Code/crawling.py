# !pip install tushare
# !pip install tqdm

import datetime

def get_nday_list(n):
    before_n_days = []
    if isinstance(n,str):
        n = (datetime.date.today()-datetime.datetime.strptime(n,"%Y%m%d").date()).days
    for i in range(1, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days

date_list = get_nday_list("20190101")
print(date_list)

import tushare as ts
import numpy as np
from tqdm import tqdm
ts.set_token('3f545dd41ef95e7da766a941dc16c58761da22450f6b70ebdff858a8')
pro = ts.pro_api()

for i in tqdm(range(len(date_list) - 1)):
    df = pro.news(src='sina', start_date=date_list[i], end_date=date_list[i+1], fields = 'datetime,content,title,channels')
    df['channels'] = df.loc[:,'channels'].apply(lambda x: ' '.join([d['name'] for d in x]))
    df.to_csv('stockDataFromTushare/sina-{}.csv'.format(date_list[i]),sep = '\t',index = False)
    
    df = pro.news(src='wallstreetcn', start_date=date_list[i], end_date=date_list[i+1], fields = 'datetime,content,title,channels')
    df['channels'] = df.loc[:,'channels'].apply(lambda x: ' '.join(x))
    df.to_csv('stockDataFromTushare/ws-{}.csv'.format(date_list[i]),sep = '\t',index = False)
    
    df = pro.news(src='10jqka', start_date=date_list[i], end_date=date_list[i+1], fields = 'datetime,content,title,channels')
    # df['channels'] = df.loc[:,'channels'].apply(lambda x: ' '.join([d['name'] for d in x]))
    df.to_csv('stockDataFromTushare/ths-{}.csv'.format(date_list[i]),sep = '\t',index = False)
    
    df = pro.news(src='eastmoney', start_date=date_list[i], end_date=date_list[i+1], fields = 'datetime,content,title,channels')
    # df['channels'] = df.loc[:,'channels'].apply(lambda x: ' '.join([d['name'] for d in x]))
    df.to_csv('stockDataFromTushare/eastmoney-{}.csv'.format(date_list[i]),sep = '\t',index = False)
    
    df = pro.news(src='yuncaijing', start_date=date_list[i], end_date=date_list[i+1], fields = 'datetime,content,title,channels')
    df['channels'] = df.loc[:,'channels'].apply(lambda x: ' '.join(x))
    df.to_csv('stockDataFromTushare/ycj-{}.csv'.format(date_list[i]),sep = '\t',index = False)

for i in tqdm(range(len(date_list) - 1)):
    df = pro.major_news(src='', start_date=date_list[i]+' 00:00:00', end_date=date_list[i+1]+' 00:00:00', fields='title,content,pub_time')

    df.to_csv('stockDataFromTushare/general-{}.csv'.format(date_list[i]),sep = '\t',index = False)

import tushare as ts
import numpy as np
from tqdm import tqdm
ts.set_token('3f545dd41ef95e7da766a941dc16c58761da22450f6b70ebdff858a8')
pro = ts.pro_api()
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
for ts_code in list(data['ts_code']):
    print("Crawling {}".format(ts_code.lower()[-2:]+ts_code[:-3]))
    df = pro.daily_basic(ts_code=ts_code, start_date = '20190101', fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
    df.to_csv('stockDataFromTushare/daily_basic/{}.csv'.format(ts_code.lower()[-2:]+ts_code[:-3]), sep = '\t',index = False)

for ts_code in list(data['ts_code']):
    print("Crawling {}".format(ts_code.lower()[-2:]+ts_code[:-3]))
    df = pro.daily(ts_code=ts_code, start_date = '20190101', fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
    df.to_csv('daily/{}.csv'.format(ts_code.lower()[-2:]+ts_code[:-3]), sep = '\t',index = False)