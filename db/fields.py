class RiakBaseField(object):
    '''
    The base for all Riak fields. It simply allows each field to have a value
    and can determine if it is allowed to be empty or not.
    '''
    
    def __init__(self, required=True, *args, **kwargs):
        self.value = None
        self.required = required


    def is_valid(self):
        if self.required and not self.value:
            return False
        return True


class RiakField(RiakBaseField):
    '''
    The value of this field will be uses to generate
    the key which will be stored in riak.
    The order determines the generation of the key
    '''

    def __init__(self, in_key=True, in_value=True, required=True, *args, **kwargs):
        if in_key and  not required:
            raise ValueError('If is a key is in_key, it has to be required')
        self.in_key = in_key
        self.in_value = in_value
        super(RiakField, self).__init__(required, *args, **kwargs)


class RiakListField(RiakField):

    def is_valid(self):
        if not isinstance(self.value, (list, tuple)):
            raise TypeError('Should be a list or a tuple')

        super(RiakListField, self).is_valid(self)
