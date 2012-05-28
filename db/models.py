from db.fields import RiakField
from db.manager import RiakManager
from db import RiakModelMissingManager
import copy
# import riak

class ModelMeta(type):

    def __new__(cls, name, bases, attr_dict):
        # Create a dict of all riak fields for deepcopy later 
        # This turns later allows these class attributes to be converted into
        # instance attributes
        field_dict = dict([(k, attr_dict.pop(k)) for k, v in attr_dict.items() if isinstance(v, RiakField)])
        # This will still be accessible at a class level
        attr_dict['base_fields'] = field_dict 


        print attr_dict
        print name
        print cls

        # Initialise the manager to hook into this class (for now the manager
        # must be called 'objects')
        try:
            manager = attr_dict['objects']
        except KeyError, e:
            raise RiakModelMissingManager('Your model must have an objects manager')
        else:
            if not isinstance(manager, RiakManager):
                raise RiakModelMissingManager('Objects is not a manager')
            # Make the manager use this class
            manager.set_model(cls, name)

        return super(ModelMeta, cls).__new__(cls, name, bases, attr_dict)

class RiakBaseModel(object):

    __metaclass__ = ModelMeta

    def __init__(self, *args, **kwargs):
        self.fields = copy.deepcopy(self.base_fields)
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

    def save(self, *args, **kwargs):
        '''
        saves a datapoint in riak
        '''
        self.validate_fields()
        self.generate_key()
        self.generate_values()
        with open(RiakModel.bucket_name, 'a') as f:
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


