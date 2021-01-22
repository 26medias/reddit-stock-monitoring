import os
import sys
import glob
import pandas as pd
import re
from datetime import datetime

def fix_path(name):
  return sys.path[0]+'/'+name

# Get the symbols
class Tickers:
  def __init__(self):
    df = pd.DataFrame()
    for filename in glob.glob(fix_path('symbols/*')):
      _df = pd.read_csv(filename, sep='\t')
      _df['source'] = re.findall(r"symbols\/([a-zA-Z]+)\.txt", filename)[0]
      df = df.append(_df)
    self.df = df.dropna()

tickers = Tickers()
df = tickers.df

# Symbols to match & ignore
real_symbols = df['Symbol'].unique()
false_symbol = ['ON','IN','AT','FOR','BY','DD','YOLO','CORP','ONE','SUB','MOON','CEO']