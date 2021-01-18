import os
import praw
import spacy
nlp  = spacy.load('en_core_web_sm',disable=['ner','textcat'])


def main():
  reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    password=reddit_password,
    user_agent='StockMonitor/0.1 by thePsychonautDad',
    username='thePsychonautDad',
  )

  subreddit = reddit.subreddit("Baystreetbets")
  for submission in subreddit.stream.submissions():
    process_submission(submission)


def process_submission(submission):
  normalized_title = submission.title.lower()
  doc = nlp(normalized_title)
  print(doc)


if __name__ == "__main__":
  main()