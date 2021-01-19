#! /usr/bin/python

import os
import praw
import spacy
nlp  = spacy.load('en_core_web_sm',disable=['ner','textcat'])
import nltk
from nltk.tokenize import word_tokenize
import glob
import pandas as pd
import re
from datetime import datetime
import threading

# Get the symbols
class Tickers:
  def __init__(self):
    df = pd.DataFrame()
    for filename in glob.glob('symbols/*'):
      print('Loading symbols from '+filename)
      _df = pd.read_csv(filename, sep='\t')
      _df['source'] = re.findall(r"^symbols\/([a-zA-Z]+)\.txt", filename)[0]
      df = df.append(_df)
      print(filename+': ', len(_df), 'Total: ', len(df))
    self.df = df.dropna()

tickers = Tickers()
df = tickers.df

# Symbols to match & ignore
real_symbols = df['Symbol'].unique()
false_symbol = ['ON','IN','AT','FOR','BY','DD','YOLO','CORP','ONE','SUB','MOON','CEO']


# Get the credentials & settings for PRAW
reddit_client_id=os.environ['reddit_client_id']
reddit_client_secret=os.environ['reddit_client_secret']
reddit_password=os.environ['reddit_password']
reddit_useragent=os.environ['reddit_useragent']
reddit_username=os.environ['reddit_username']


# Monitor Reddit
class Monitor:
  def __init__(self):
    print("Monitoring")
    self.df = False
    self.df_name = False
    if os.path.exists('datasets.pkl'):
      self.datasets = pd.read_pickle('datasets.pkl')
    else:
      self.datasets = pd.DataFrame()
  
  def start(self):
    self.praw = praw.Reddit(
      client_id=reddit_client_id,
      client_secret=reddit_client_secret,
      password=reddit_password,
      user_agent=reddit_useragent,
      username=reddit_username
    )
    self.subreddit = self.praw.subreddit("wallstreetbets")
    
    self.commentThread = threading.Thread(name='comments', target=self.monitorComments)
    self.submissionThread = threading.Thread(name='submissions', target=self.monitorSubmissions)
    self.commentThread.start()
    self.submissionThread.start()
    
    
  def monitorSubmissions(self):
    for submission in self.subreddit.stream.submissions():
      self.process_submission(submission)
  
  def monitorComments(self):
    for comment in self.subreddit.stream.comments():
      self.process_comment(comment)

  def process_submission(self, submission):
    NER = nlp(submission.title.lower())
    NER2 = nlp(submission.selftext.lower())
    found = []
    has_rocket = '🚀' in submission.title.lower()
    for token in NER:
      if '.' in token.text:
        w = token.text.upper().split('.')[0]
      else:
        w = token.text.upper()
      if token.pos_ in ['ADP','NOUN','PROPN'] and w in real_symbols and w not in false_symbol:
        found.append(w)
    for token in NER2:
      if '.' in token.text:
        w = token.text.upper().split('.')[0]
      else:
        w = token.text.upper()
      if token.pos_ in ['ADP','NOUN','PROPN'] and w in real_symbols and w not in false_symbol:
        found.append(w)
    if (len(found)>0):
      #print('\n\n----------------')
      #print(has_rocket, submission.title)
      #print(found)
      self.record(source='submission', has_rocket=has_rocket, symbols=list(set(found)), title=submission.title)
  
  def process_comment(self, comment):
    NER = nlp(comment.body.lower())
    found = []
    has_rocket = '🚀' in comment.body.lower()
    for token in NER:
      if '.' in token.text:
        w = token.text.upper().split('.')[0]
      else:
        w = token.text.upper()
      if token.pos_ in ['ADP','NOUN','PROPN'] and w in real_symbols and w not in false_symbol:
        found.append(w)
    if (len(found)>0):
      self.record(source='comment', has_rocket=has_rocket, symbols=list(set(found)), title=comment.body)
    
  def get_df(self):
    d = datetime.now()
    dname = '{}-{}-{}_{}_{}'.format(d.year,d.month,d.day,d.hour,d.minute)
    filename = "data/"+dname+".pkl"
    if self.dname != False:
      filename_prev = "data/"+self.dname+".pkl"
    if self.df_name != dname:
      # Save to the index
      self.datasets.at[datetime.timestamp(d), 'filename'] = filename
      self.datasets.to_pickle('datasets.pkl')
      print("#### New DF: ", filename)
      # Save the previous df?
      if self.df_name != False:
        self.df.to_pickle(filename_prev)
        self.df = False # Delete the df
      # Create a new df
      if os.path.exists(filename):
        # Recover existing file
        print("#### Recovering DF: ", filename)
        self.df = pd.read_pickle(filename)
        self.df_name = dname
      else:
        # Create a new DF
        self.df = pd.DataFrame(columns=['comment', 'submission', 'rockets'])
        self.df_name = dname
      self.df.to_pickle(filename)
    return self.df

  def record(self, source, has_rocket, symbols, title=''):
    print(source, has_rocket, symbols)
    df = self.get_df()
    for symbol in symbols:
      if symbol in df.index:
        df.at[symbol, source] = df.at[symbol, source]+1
        if has_rocket:
          df.at[symbol, 'rockets'] = df.at[symbol, 'rockets']+1
        else:
          df.at[symbol, 'rockets'] = 0
      else:
        df.at[symbol, source] = 1
        if has_rocket:
          df.at[symbol, 'rockets'] = 1
        else:
          df.at[symbol, 'rockets'] = 0

reddit = Monitor()
reddit.start()