import os
import pandas as pd

stock_code_list =[]
for root,dirs,files in os.walk('/home/xiaofang/tmp/stock/data/day/'):
    if files:
        for f in files:
            if '.csv' in f:
                stock_code_list.append(f.split('.csv')[0])

for code in stock_code_list:
    print code
    stock_data = pd.read_csv('~/tmp/stock/data/day/' + code + '.csv', index_col=0,parse_dates=[0])
    stock_data.sort_index()
    print stock_data.head()

    period = 'W'
    period_stock_data = stock_data.resample(period,how='last')
    period_stock_data['open']= stock_data['open'].resample(period,how='first')
    period_stock_data['high'] = stock_data['high'].resample(period, how='max')
    period_stock_data['low'] = stock_data['low'].resample(period, how='min')
    period_stock_data['volume'] = stock_data['volume'].resample(period, how='sum')
    period_stock_data['amount'] = stock_data['amount'].resample(period, how='sum')

    period_stock_data = period_stock_data[period_stock_data['close'].notnull()]
    period_stock_data.to_csv('~/tmp/stock/data/week/' + code + '.csv')
