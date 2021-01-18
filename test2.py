import os
import praw


def main():
	reddit = praw.Reddit(
		client_id=os.environ['reddit_client_id'],
		client_secret=os.environ['reddit_client_secret'],
		password=os.environ['reddit_password'],
		user_agent='StockMonitor/0.1 by thePsychonautDad',
		username='thePsychonautDad',
	)

	subreddit = reddit.subreddit("AskReddit")
	for submission in subreddit.stream.submissions():
		process_submission(submission)


def process_submission(submission):
	normalized_title = submission.title.lower()
	print(normalized_title)


if __name__ == "__main__":
	main()