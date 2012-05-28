class RiakManager(object):
    def __init__(self):
        self.model = None

    def set_model(self, model_cls, name):
        '''
        This method is called in ModelMeta and sets the manager to be used for
        the given model. It also sets the a class variable for the bucket name
        based on the model name
        '''
        self.model = model_cls
        # For now, we simply use the name of the class lower cased
        self.model.bucket_name = name.lower()


    
