import riak

class RiakModelBaseError(Exception):
    def __init__(self, message):
        self.message = message

class RiakModelMissingManager(RiakModelBaseError):
    pass


riak_client = riak.Client(host='10.177.0.81')
