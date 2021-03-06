from datetime import datetime


class Loan(object):
    def __init__(self, init_object=None, sql_params=None, **kwargs):
        self.sql_params = sql_params

        if init_object is None:
            self.init_from_keyval_type(kwargs)

        else:
            raise Exception('invalid init_object for loan initialization')


    def init_from_keyval_type(self, keyval):
        self.loan_id = keyval['loan_id']
        self.retrieval_datetime = keyval.get('retrieval_datetime', datetime.utcnow())
        self.amount = keyval['amount']
        self.lender_id = keyval['lender_id']
        self.borrower_id = keyval['borrower_id']
        self.source_submission_id = keyval['source_submission_id']
        self.source_comment_id = keyval['source_comment_id']
