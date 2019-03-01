from datetime import datetime


class LoanRequest(object):
    def __init__(self, init_object=None, sql_params=None, **kwargs):
        self.sql_params = sql_params

        if init_object is None:
            self.init_from_keyval_type(kwargs)

        else:
            raise Exception('invalid init_object for loan request initialization')


    def init_from_keyval_type(self, keyval):
        self.request_id = keyval['request_id']
        self.retrieval_datetime = keyval.get('retrieval_datetime', datetime.utcnow())
        self.source_submission_id = keyval['source_submission_id']
        self.req_datetime = keyval['req_datetime']
        self.insertion_datetime = keyval['insertion_datetime']
        self.return_date = keyval['return_date']
        self.borrower_id = keyval['borrower_id']
        self.borrower_location = keyval['borrower_location']
        self.principal_amt = keyval['principal_amt']
        self.repay_amt = keyval['repay_amt']
        self.prearranged = keyval['prearranged']
        self.takes_paypal = keyval['takes_paypal']
        self.takes_venmo = keyval['takes_venmo']
        self.takes_zelle = keyval['takes_zelle']
