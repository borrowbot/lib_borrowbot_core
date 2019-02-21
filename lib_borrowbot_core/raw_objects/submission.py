import praw
import pandas as pd
from datetime import datetime


class Submission(object):
    """ A class representing a single reddit submission. The class provides standard attributes and can be initialized
        in a variety of different ways for different use cases. Read-only SQL interfaces are also provided for comment
        retrieval, initalization from submission_id, and consistency validation.

    Initialization Patterns:
        1. Directly using init arguments.
        2. From a praw.models.Submission class.
        3. From a dict-like object (for instance a pandas Series representing a single row of a DataFrame queried from
            the submissions table).
        4. From a string representing a submission_id.

    Comments:
        A submission class provides and attribute to store associated comments. Comments can be passed in directly at
        initialization time (they will not be validated) or they can be retrived automatically from the SQL table using
        the retrieve_comments method. The comment attribute where comments are stored will be set to None if comments
        were not provided or retrieved to distinguish between the case where a submission simply has no commetns.

    SQL Parameters:
        Passing in SQL parameters with initialization is optional in most cases but having valid SQL parameters stored
        in the sql_params attribute is required for initialization via submission_id and for automatic retrival of
        submission comments. It should be noted that methods that make use of the SQL interfaces are not intended for
        any sort of bulk operation as the queries would likely overload the SQL instance.
        # TODO: Add bulk methods to the lib_borrowbot_core library.

    Validation:
        A Submission object exposes a number of methods that can be used to check the consistency of a submission object
        against data stored in the SQL tables in addition to providing some sanity checking of the table data itself.
        For instance, checking that the number of stored comments matches the stored comment counter or that comment
        creation times always follow submission creation times etc.

    Args:
        init_object
        comments <list>
        sql_params <dict>
        **kwargs
    """

    def __init__(self, init_object=None, comments=None, sql_params=None, **kwargs):
        self.comments = None
        self.sql_params = sql_params

        # Init from arguments
        if init_object is None:
            self.init_from_keyval_type(kwargs)

        # Init from DataFrame row
        if isinstance(init_object, pd.core.series.Series):
            self.init_from_keyval_type(init_object)

        # Init from submission_id
        if isinstance(init_object, str):
            self.init_from_submission_id(init_object)

        # Init from praw Submission object
        if isinstance(init_object, praw.models.Submission):
            self.init_from_praw_submission(init_object)

        # Some unexpected author responses from PRAW. Need to look more into this.
        # self.validate_object(query=False)


    def init_from_keyval_type(self, keyval_store):
        self.submission_id = keyval['submission_id']
        self.retrieval_datetime = keyval.get('retrieval_datetime', datetime.utcnow())
        self.creation_datetime = keyval['creation_datetime']
        self.score = keyval['score']
        self.num_comments = keyval['num_comments']
        self.url = keyval['url']
        self.upvote_ratio = keyval['upvote_ratio']
        self.permalink = keyval['permalink']
        self.subreddit_name = keyval['subreddit_name']
        self.subreddit_id = keyval['subreddit_id']
        self.title = keyval['title']
        self.text = keyval['text']
        self.author_name = keyval.get('author_name')
        self.author_id = keyval.get('author_id')


    def init_from_praw_submission(self, praw_submission):
        self.submission_id = 't3_' + praw_submission.id
        self.retrieval_datetime = datetime.utcnow()
        self.creation_datetime = datetime.utcfromtimestamp(praw_submission.created_utc)
        self.score = praw_submission.score
        self.num_comments = praw_submission.num_comments
        self.url = praw_submission.url
        self.upvote_ratio = praw_submission.upvote_ratio
        self.permalink = praw_submission.permalink
        self.subreddit_name = praw_submission.subreddit
        self.subreddit_id = praw_submission.subreddit.name
        self.title = praw_submission.title
        self.text = praw_submission.selftext

        # Try/except blocks to cover deleted authors
        # Submission.author can be either None or can be a Redditor class which throws a 404 when the fullname attribute
        # is asked for.
        try:
            self.author_name = praw_submission.author.name
        except:
            self.author_name = None
        try:
            self.author_id = praw_submission.author.fullname
        except:
            self.author_id = None


    def init_from_submission_id(self):
        pass


    def retrieve_comments(self):
        pass


    def validate_object(self, query=False):
        # TODO: Query consistency checks
        # TODO: Check num comments match and creation times are appropriate
        assert type(self.author_name) == type(self.author_id)
