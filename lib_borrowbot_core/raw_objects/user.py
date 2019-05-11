class User(object):
    """ A simple wrapper class for writing a user's id to name lookup to the user_lookup table.
    """
    def __init__(self, user_id, user_name):
        self.user_id = user_id
        self.user_name = user_name
