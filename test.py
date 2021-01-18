import os
import praw
import time

from bottr.bot import CommentBot

def parse_comment(comment):
    """Define what to do with a comment"""
    if 'banana' in comment.body:
        print('ID: {}'.format(comment.id))
        print('Author: {}'.format(comment.author))
        print('Body: {}'.format(comment.body))

if __name__ == '__main__':

    # Get reddit instance with login details
    reddit = praw.Reddit(client_id=os.environ['reddit_client_id'],
                         client_secret=os.environ['reddit_client_secret'],
                         password=os.environ['reddit_password'],
                         user_agent='StockMonitor/0.1 by thePsychonautDad',
                         username='thePsychonautDad')

    # Create Bot with methods to parse comments
    bot = CommentBot(reddit=reddit,
                    func_comment=parse_comment,
                    subreddits=['Baystreetbets'])

    # Start Bot
    bot.start()

    # Run bot for 10 minutes
    time.sleep(30)

    # Stop Bot
    bot.stop()