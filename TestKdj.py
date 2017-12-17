import os
import pandas as pd
stock_code_list =[]
for root,dirs,files in os.walk('/home/xiaofang/tmp/stock/data/week/'):
    if files:
        for f in files:
            if '.csv' in f:
                stock_code_list.append(f.split('.csv')[0])

all_stock = pd.DataFrame()
for code in stock_code_list:
    print code
    stock_data = pd.read_csv('~/tmp/stock/data/week/'+code +'.csv',index_col=0)
    stock_data.sort_index()
    #print stock_data.head()

    low_list = stock_data['low'].rolling(window=9,center=False).min()
    low_list.fillna(value=stock_data['low'].expanding(min_periods=1).min(),inplace=True)
    high_list = stock_data['high'].rolling(window=9,center=False).max()
    high_list.fillna(value=stock_data['high'].expanding(min_periods=1).max(),inplace=True)
    rsv = (stock_data['close']-low_list)/(high_list -low_list) *100
    stock_data['KDJ_K'] = pd.ewma(rsv,com=2)
    stock_data['KDJ_D'] = pd.ewma(stock_data['KDJ_K'],com=2)
    stock_data['KDJ_J'] = 3*stock_data['KDJ_K'] - 2 * stock_data['KDJ_D']

    # gold
    stock_data['KDJ_GOLD_DEAD'] = ''
    kdj_position = stock_data['KDJ_K'] > stock_data['KDJ_D']
    stock_data.loc[kdj_position[(kdj_position == True & (kdj_position.shift() == False))].index,'KDJ_GOLD_DEAD']='gold'
    stock_data.loc[kdj_position[(kdj_position == False & (kdj_position.shift() == True))].index, 'KDJ_GOLD_DEAD'] = 'dead'

    for n in [1,2,3,5,10,20]:
        stock_data['next' + str(n) ] = stock_data['close'].shift(-1*n)/stock_data['close'] - 1.0

    stock_data.dropna(how='any',inplace=True)

    stock_data= stock_data[(stock_data['KDJ_GOLD_DEAD'] == 'gold') & (stock_data['KDJ_K'] < 20)]
    if stock_data.empty:
        continue
    all_stock = all_stock.append(stock_data,ignore_index=True)
print
print 'gold count %d: ' %all_stock.shape[0]

print()

for n in [1,2,3,5,10,20]:
    print 'gold after %d: ' %n
    print 'avg is %.2f%%,' %(all_stock['next'+str(n)].mean()*100)
    print 'raise persent: %.2f%% ' % (all_stock[(all_stock['next'+str(n)] >0)].shape[0]/float(all_stock.shape[0])*100)
    print