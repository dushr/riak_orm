from db.fields import RiakField
import copy
import riak

class DerivedRiakFieldInitiator(type):

    def __new__(cls, name, bases, attr_dict):
        field_dict = dict([(k, attr_dict.pop(k)) for k, v in attr_dict.items() if isinstance(v, RiakField)])
        attr_dict['base_fields'] = field_dict 

        return super(DerivedRiakFieldInitiator, cls).__new__(cls, name, bases, attr_dict)



class RiakBaseModel(object):

    __metaclass__ = DerivedRiakFieldInitiator


    def __init__(self, *args, **kwargs):
        self.fields = copy.deepcopy(self.base_fields)
        self.bucket = self.__class__.__name__.lower()
        for k,v in self.fields.iteritems():
            v.value = kwargs.get(k, None)
            setattr(self, k, v.value)

    def __iter__(self):
        for name in self.fields:
            yield self[name]

    def __getitem__(self, name):
        
        return self.fields[name]

    def __setattr__(self, name, value):
        
        if name in getattr(self, 'fields', ()):
            self.fields[name].value = value

        super(RiakBaseModel, self).__setattr__(name, value)

            

    def save(self, *args, **kwargs):
        '''
        saves a datapoint in riak
        '''
        raise NotImplementedError

    def validate_fields(self, *args, **kwargs):
        '''
        Validates the fields
        '''
        raise NotImplementedError



class RiakModel(RiakBaseModel):

    # objects = RiakObjectManager(bucket)

    def save(self, *args, **kwargs):
        '''
        saves a datapoint in riak
        '''
        self.validate_fields()
        self.generate_key()
        self.generate_values()
        with open(self.bucket, 'a') as f:
            f.write('%s:%s\n' % (self.key, self.values))

        pass

    def validate_fields(self, *args, **kwargs):
        for name, field in self.fields.iteritems():
            if not field.is_valid():
                raise AttributeError('%s field is required' % name)

    def generate_key(self, *args, **kwargs):
        key_list = [str(self[attr].value) for attr in self.key_order if self[attr].in_key]
        self.key = self.key_seperator.join(key_list)
        return self.key

    def generate_values(self, *args, **kwargs):
        key_dict = dict([(name, field.value) for name, field in self.fields.iteritems() if field.in_value])
        self.values = key_dict
        return self.values



class RiakObjectManager(object):

    def __init__(self, bucket):
        self.bucket = bucket

    def filter(self, *args, **kwargs):
        pass

