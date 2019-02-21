import praw
import pandas as pd
from datetime import datetime


class Comment(object):
    def __init__(self, init_object=None, submission=None, sql_params=None, **kwargs):
        self.retrieval_datetime = datetime.utcnow()
        self.submission = submission
        self.sql_params = sql_params

        # Init from arguments
        if init_object is None:
            self.init_from_keyval_type(kwargs)

        # Init from DataFrame row
        if isinstance(init_object, pd.core.series.Series):
            self.init_from_keyval_type(init_object)

        # Init from submission_id
        if isinstance(init_object, str):
            self.init_from_comment_id(init_object)

        # Init from praw Submission object
        if isinstance(init_object, praw.models.reddit.comment.Comment):
            self.init_from_praw_comment(init_object)

        # Some unexpected author responses from PRAW. Need to look more into this.
        # self.validate_object(query=False)


    def init_from_keyval_type(self, keyval_store):
        self.comment_id = keyval_store['comment_id']
        self.retrieval_datetime = keyval.get('retrieval_datetime', datetime.utcnow())
        self.creation_datetime = keyval_store['creation_datetime']
        self.score = keyval_store['score']
        self.subreddit_name = keyval_store['subreddit_name']
        self.subreddit_id = keyval_store['subreddit_id']
        self.link_id = keyval_store['link_id']
        self.parent_id = keyval_store['parent_id']
        self.text = keyval_store['text']
        self.author_name = keyval.get('author_name')
        self.author_id = keyval.get('author_id')


    def init_from_praw_comment(self, praw_comment):
        self.comment_id = 't1_' + praw_comment.id
        self.retrieval_datetime = datetime.utcnow()
        self.creation_datetime = datetime.utcfromtimestamp(praw_comment.created_utc)
        self.score = praw_comment.score
        self.subreddit_name = praw_comment.subreddit
        self.subreddit_id = praw_comment.subreddit.name
        self.link_id = praw_comment.link_id
        self.parent_id = praw_comment.parent_id
        self.text = praw_comment.body

        # Try/except blocks to cover deleted authors
        try:
            self.author_name = praw_comment.author.name
        except:
            self.author_name = None
        try:
            self.author_id = praw_comment.author.fullname
        except:
            self.author_id = None


    def init_from_comment_id(self, comment_id):
        pass


    def retrieve_submission(self):
        pass


    def validate_object(self, query=False):
        # TODO: Query consistency checks
        # TODO: More local checks
        assert type(self.author_name) == type(self.author_id)
