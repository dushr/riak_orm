class RiakModelBaseError(Exception):
    def __init__(self, message):
        self.message = message

class RiakModelMissingManager(RiakModelBaseError):
    pass
