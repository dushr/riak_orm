import riak

class RiakModelBaseError(Exception):
    def __init__(self, message):
        self.message = message

class RiakModelMissingManager(RiakModelBaseError):
    pass

class DoesNotExistError(RiakModelBaseError):
    pass


riak_client = riak.RiakClient(host='10.177.0.81')
