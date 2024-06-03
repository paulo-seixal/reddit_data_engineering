import configparser
import pathlib
import praw
from prawcore.exceptions import ResponseException
from praw.models import MoreComments
import pandas as pd
import numpy as np
import sys
import datetime

# Get the path of the script
script_path = pathlib.Path(__file__).parent.resolve()
datasets_path = pathlib.Path(__file__).parent.parent.resolve() / 'datasets'


# Read the config file
parser = configparser.ConfigParser()
parser.read(script_path / 'config.ini')

# Get the config variables
client_id = parser.get('REDDIT_APP', 'client_id')
client_secret = parser.get('REDDIT_APP', 'client_secret')
redirect_uri = parser.get('REDDIT_APP', 'redirect_uri')
username = parser.get('REDDIT_APP', 'username')
password = parser.get('REDDIT_APP', 'password')


output_name = datetime.datetime.now().strftime("%Y%m%d")



FIELDS = ['id', 
          'title',
          'score',
          'edited',
          'num_comments',
          'author',
          'created_utc',
          'url',
          'upvote_ratio',
          'over_18',
          'stickied']


def connect():
    '''
    Connect to the Reddit API
    '''
    try:
        reddit = praw.Reddit(client_id=client_id, 
                             client_secret=client_secret,
                             redirect_uri=redirect_uri,
                             user_agent='testscript by /u/username')
        reddit.read_only = True
        return reddit
    except ResponseException as e:
        print(f'Unable to connect to Reddit API: {e}')

def get_subreddit_posts(reddit_api_instance, subreddit, time_filter='week' ,limit=None):
    '''
    Get the posts from a subreddit
    '''
    try:
        subreddit = reddit_api_instance.subreddit(subreddit).hot(limit=limit)
        # posts = subreddit.top(time_filter=time_filter ,limit=limit)
        return subreddit
    except Exception as e:
        print(f'Unable to get posts from subreddit: {e}')

def get_comments_from_post(reddit_api_instance, post_id):
    '''
    Get the comments from a post
    '''
    comments = []
    try:
        submission = reddit_api_instance.submission(id=post_id)
        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, MoreComments):
                continue
            comments.append(top_level_comment.body)
        return comments
    except Exception as e:
        print(f'Unable to get comments from post: {e}')


def get_posts_info_to_df(posts):
    '''
    Get the information of the posts and return a DataFrame
    '''
    data = []
    try:
        for post in posts:
            post_info = {field: getattr(post, field) for field in FIELDS}
            data.append(post_info)
        return pd.DataFrame(data)
    except Exception as e:
        print(f'Unable to get posts information: {e}')


def transform_df(df):
    '''
    Transform the DataFrame
    '''
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df['over_18'] = np.where((df['over_18'] == False) | (df['over_18'] == 'False'), False, True).astype(bool)
    df['stickied'] = np.where((df['stickied'] == False) | (df['stickied'] == 'False'), False, True).astype(bool)
    df['edited'] = np.where((df['edited'] == False) | (df['edited'] == 'False'), False, True).astype(bool)
    return df


def load_df_to_csv(df, path):
    '''
    Load the DataFrame to a CSV file
    '''
    try:
        df.to_csv(path, index=False)
    except Exception as e:
        print(f'Unable to save DataFrame to CSV: {e}')


def main():
    reddit_instance = connect()
    posts = get_subreddit_posts(reddit_instance, 'portugal', limit=20)
    df = get_posts_info_to_df(posts)
    df = transform_df(df)
    load_df_to_csv(df, datasets_path / f'{output_name}.csv')


if __name__ == '__main__':
    main()

    