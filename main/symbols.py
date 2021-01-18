import glob
import pandas as pd
import re

class Tickers:
  def __init__(self):
    df = pd.DataFrame()
    for filename in glob.glob('symbols/*'):
      _df = pd.read_csv(filename, sep='\t')
      _df['source'] = re.findall(r"^symbols\/([a-zA-Z]+)\.txt", filename)[0]
      df = df.append(_df)
    self.df = df.dropna()

tickers = Tickers()
symbols = tickers.df