import praw
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


class Comment(object):
    """ This class is very similar to the Submission class found in lib_borrowbot_core.raw_objects.submission.Submission
        and so one should refer to the documentation there for help.
    """
    def __init__(self, init_object=None, submission=None, sql_params=None, **kwargs):
        self.submission = submission
        self.sql_params = sql_params

        # Init from arguments
        if init_object is None:
            self.init_from_keyval_type(kwargs)

        # Init from DataFrame row
        elif isinstance(init_object, pd.core.series.Series):
            self.init_from_keyval_type(init_object)

        # Init from comment_id
        elif isinstance(init_object, str):
            self.init_from_comment_id(init_object)

        # Init from praw Submission object
        elif isinstance(init_object, praw.models.reddit.comment.Comment):
            self.init_from_praw_comment(init_object)

        else:
            raise Exception('invalid init_object for comment initialization')


    def init_from_keyval_type(self, keyval):
        self.comment_id = keyval['comment_id']
        self.retrieval_datetime = keyval.get('retrieval_datetime', datetime.utcnow())
        self.creation_datetime = keyval['creation_datetime']
        self.score = keyval['score']
        self.subreddit_name = keyval['subreddit_name']
        self.subreddit_id = keyval['subreddit_id']
        self.link_id = keyval['link_id']
        self.parent_id = keyval['parent_id']
        self.text = keyval['text']
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
        # Submission.author can be either None or can be a Redditor class which throws a 404 when the fullname attribute
        # is asked for.
        try:
            self.author_name = praw_comment.author.name
        except:
            self.author_name = None
        try:
            self.author_id = praw_comment.author.fullname
        except:
            self.author_id = None


    def init_from_comment_id(self, comment_id):
        if self.sql_params is None:
            raise Exception("cannot get comment from comment ID without SQL params")

        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()

        query = 'SELECT * FROM comments WHERE comment_id = "{}"'.format(comment_id)
        df = pd.read_sql(sql=query, con=con)
        con.close()

        if df.shape[0] != 1:
            raise Exception("could not find comment with comment ID {}".format(comment_id))
        self.init_from_keyval_type(df.iloc[0])


    def retrieve_submission(self):
        pass


    def validate_object(self):
        """ Performs some basic checks on the consistency of the information stored in a Submission object. Note that
            the num_comments counter occassionally underestimates the true number of comments so this method is not a
            perfect determination of correctness.
        """
        if self.author_name is None:
            assert self.author_id is None
