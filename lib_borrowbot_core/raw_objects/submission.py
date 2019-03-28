import praw
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from lib_borrowbot_core.raw_objects.comment import Comment


def bulk_retrieve_comments(submission_list, sql_params):
    """ To reduce the number of SQL queries need to retrieve comments for a large number of Submission objects, this
        function is provided to populate the comments attribute for a potentially large list of Submission objects with
        only a single SQL query.

    Args:
        submission_list [<Submission>]: A list of Submission objects to retrieve comments for. If comments are already
            present for any of these Submission objects, they will be overwritten. If only a single Submission object
            is provided, the Submission object's sql_params will be overwritten with the set passed into this function.
        sql_params <dict>: A dictionary of the SQL parameters to use for the comment retrieval query.

    Rets [<Submission>]: Returns the same list of Submission objects as is passed in. Note that this function edits the
        Submission list in place.
    """
    if len(submission_list) == 0:
        return submission_list

    if len(submission_list) == 1:
        submission_list[0].sql_params = sql_params
        submission_list[0].retrieve_comments()
        return submission_list

    engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
        sql_params['user'],
        sql_params['passwd'],
        sql_params['host'],
        sql_params['db']
    ), convert_unicode=True, encoding='utf-8')
    con = engine.connect()

    submission_ids = [s.submission_id for s in submission_list]
    query = 'SELECT * FROM comments WHERE link_id IN {}'.format(tuple(submission_ids))
    df = pd.read_sql(sql=query, con=con)
    con.close()

    for s in submission_list:
        s.comments = [Comment(init_object=c[1]) for c in df[df['link_id'] == s.submission_id].iterrows()]

    return submission_list


class Submission(object):
    """ A class representing a single reddit submission. The class provides standard attributes and can be initialized
        in a variety of different ways for different use cases. Read-only SQL interfaces are also provided for comment
        retrieval, initalization from submission_id, and consistency validation.

    Initialization Patterns:
        1. Directly using init arguments.
        2. From a praw.models.Submission object.
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
        init_object: An object used to initialize the Submission object. This can be a submission_id string, a key-value
            store (eg. a pandas series or a dictionary), or a PRAW submission object. If no init_object is provided, the
            necessary attributes of the Submission class must be passed in as keyword arguments.
        comments [<lib_borrowbot_core.raw_objects.comment.Comment>]: A list of comment objects linked to the submission.
            These comments are not checked for correctness. If no comments are passed in, then the comments attribute
            will be initialized as None signifying that the comments have not yet been retrieved.
        sql_params <dict>: A dictionary containing SQL parameters which can be used to retrieve comments or submission
            information from a submission_id.
        **kwargs: Additional keyword arguments are use to initialize a Submission object if no init_object is passed in.
    """

    def __init__(self, init_object=None, comments=None, sql_params=None, **kwargs):
        self.comments = None
        self.sql_params = sql_params

        # Init from arguments
        if init_object is None:
            self.init_from_keyval_type(kwargs)

        # Init from DataFrame row
        elif isinstance(init_object, pd.core.series.Series):
            self.init_from_keyval_type(init_object)

        # Init from submission_id
        elif isinstance(init_object, str):
            self.init_from_submission_id(init_object)

        # Init from PRAW Submission object
        elif isinstance(init_object, praw.models.Submission):
            self.init_from_praw_submission(init_object)

        else:
            raise Exception('invalid init_object for submission initialization')


    def init_from_keyval_type(self, keyval):
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


    def init_from_submission_id(self, submission_id):
        if self.sql_params is None:
            raise Exception("cannot get submission from submission ID without SQL params")

        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()

        query = 'SELECT * FROM submissions WHERE submission_id = "{}"'.format(submission_id)
        df = pd.read_sql(sql=query, con=con)
        con.close()

        if df.shape[0] != 1:
            raise Exception("could not find submission with submission ID {}".format(submission_id))
        self.init_from_keyval_type(df.iloc[0])


    def retrieve_comments(self):
        """ Retrieves comments from the SQL database to populate the comments attribute of the Submission class. Before
            comments are retrieved, this attribute will be None to distinguish from the case where a submission has no
            comments. This method fails if the sql_params attribute is not defined.
        """
        if self.sql_params is None:
            raise Exception("cannot retrieve submission comments without SQL params")

        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()

        query = 'SELECT * FROM comments WHERE link_id = "{}"'.format(self.submission_id)
        df = pd.read_sql(sql=query, con=con)
        con.close()

        self.comments = [Comment(init_object=row[1], submission=self) for row in df.iterrows()]


    def validate_object(self):
        """ Performs some basic checks on the consistency of the information stored in a Submission object. Note that
            the num_comments counter occassionally underestimates the true number of comments so this method is not a
            perfect determination of correctness.
        """
        if self.comments is not None:
            assert len(self.comments) == self.num_comments

        if self.author_name is None:
            assert self.author_id is None

        return True
