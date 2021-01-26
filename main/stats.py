#!/usr/bin/python
import os
import sys
import glob
import pandas as pd
import re
from datetime import datetime

dev_mode = False

def fix_path(name):
  if dev_mode == True:
    return name
  return sys.path[0]+'/'+name

# Get the symbols
class Tickers:
  def __init__(self):
    df = pd.DataFrame()
    for filename in glob.glob(fix_path('datasets/symbols/*')):
      _df = pd.read_csv(filename, sep='\t')
      _df['source'] = re.findall(r"symbols\/([a-zA-Z]+)\.txt", filename)[0]
      df = df.append(_df)
    self.df = df.dropna()

tickers = Tickers()
df = tickers.df

# Symbols to match & ignore
real_symbols = df['Symbol'].unique()
false_symbol = ['ON','IN','AT','FOR','BY','DD','YOLO','CORP','ONE','SUB','MOON','CEO','OUT','INTO','MAN','POST','BRO','LIFE','CALL','DUDE','IDEA']

import os
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta

class Explorer:
  def __init__(self):
    self.ds_indexes = pd.read_pickle('datasets/datasets.pkl')
  
  def buildMatrix(self, limit=None, markets=None):
    print("buildMatrix()", limit, markets)
    ds = {}
    symbols = []

    
    
    if limit is not None:
      deltaT = datetime.now() - timedelta(hours = limit)
      t = deltaT.strftime('%Y-%m-%d %H')
      cp = self.ds_indexes.copy()
      cp.index = cp.index.astype(int)
      cp.index = cp.index * 1000000000
      cp.index = pd.to_datetime(cp.index)
      cp = cp.loc[t:]
    else:
      cp = self.ds_indexes.copy()
      cp.index = cp.index.astype(int)
      cp.index = cp.index * 1000000000
      cp.index = pd.to_datetime(cp.index)

    limit_symbols = None
    if markets is not None:
      limit_symbols = list(tickers.df[tickers.df['source'].isin(markets)]['Symbol'].unique())

    # Load the datasets
    for index, row in cp.iterrows():
      filename = 'datasets/'+row['filename'].replace('/home/julien/mk2/main/datasets/','')
      if os.path.exists(filename):
        ds[index] = pd.read_pickle(filename)
        if len(ds[index])>0:
          if limit_symbols is not None:
          	ds[index] = ds[index][ds[index].index.isin(limit_symbols)]
          ds[index]['total'] = ds[index]['comment']+ds[index]['submission']
          symbols = symbols + list(ds[index].index)
    # List the unique symbols
    symbols = list(set(symbols))
    symbols.sort()
    # Create the matrix
    matrix = pd.DataFrame(index=symbols)
    for index, row in cp.iterrows():
      if index in ds and len(ds[index])>0 and 'total' in ds[index] and 'comment' in ds[index] and 'submission' in ds[index]:
        ts = index.to_pydatetime()
        matrix[ts] = ds[index]['total']
    matrix = matrix.fillna(0)
    matrix = matrix.T
    return matrix

  def aggr(self, matrix, freq='1h'):
    return matrix.groupby(pd.Grouper(freq=freq)).sum()
  
  def top(self, matrix, n=20):
    topx   = list(matrix.sum().sort_values(ascending=False).head(n).index)
    matrix[topx].plot(figsize=(30,15))
  
  # Rank a matrix by date
  def rank(self, matrix):
    #return matrix
    output = pd.DataFrame(index=matrix.columns)
    for index, row in matrix.iterrows():
      cp = row.copy()
      sorted = cp[~cp.index.isin(false_symbol)]
      sorted = sorted.sort_values(ascending=False)
      sorted = pd.DataFrame(data=sorted)
      sorted['position'] = range(1, len(sorted)+1)
      #print(sorted)
      output[index] = sorted['position']
    output = output[~output.index.isin(false_symbol)]
    return output.T
  
  def top_ranking(self, matrix, freq='1h', delay=-1):
    reduced = explorer.aggr(matrix, freq=freq)
    ranked = explorer.rank(matrix=reduced)
    #return ranked
    index = ranked.index[delay]
    print(index)
    output = pd.DataFrame(index=ranked.T.index)
    output['rank'] = ranked.T[index]
    output['rank_diff'] = ranked.diff().T[index]
    output['count'] = reduced.T[index]
    output.sort_values(ascending=False, by='count', inplace=True)
    return output
  
  def top_rising(self, matrix, h=1, n=20):
    top = self.top_ranking(matrix, freq=str(h)+'h', delay=-1)
    df = top
    df = top[top['count']>h*n]
    df = df[df['rank_diff']<0]
    df['score'] = (df['rank'].max()-df['rank'])/df['rank'].max() * (df['rank_diff']*-1)
    return df.sort_values(ascending=False, by='score')



explorer = Explorer()

print("args: ", len(sys.argv), " - ", sys.argv)

limit_market = None
if len(sys.argv)>=4 and sys.argv[3] != 'None':
  limit_market = [sys.argv[3]]

matrix = explorer.buildMatrix(limit=72, markets=limit_market)

if sys.argv[3] == "top":
  # python3 stats.py top 24h
  # python3 stats.py top 24h OTCBB
  print(explorer.top_ranking(matrix, freq=sys.argv[2], delay=-1).head(30))
elif sys.argv[3] == "rising":
  # python3 stats.py rising 24h
  # python3 stats.py rising 12 OTCBB
  # python3 stats.py rising 12 OTCBB 5
  _n = 10
  if len(sys.argv)>=5 and sys.argv[4] != 'None':
  	_n = int(sys.argv[4])
  print(explorer.top_rising(matrix, h=int(sys.argv[2]), n=_n).head(30))