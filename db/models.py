from db.fields import RiakField
from db.manager import RiakManager
from db import RiakModelMissingManager
import copy
from db import riak_client
# import riak

class ModelMeta(type):

    def __new__(cls, name, bases, attr_dict):
        # Create a dict of all riak fields for deepcopy later 
        # This turns later allows these class attributes to be converted into
        # instance attributes
        field_dict = dict([(k, attr_dict.pop(k)) for k, v in attr_dict.items() if isinstance(v, RiakField)])
        # This will still be accessible at a class level
        attr_dict['base_fields'] = field_dict 

        model_class = super(ModelMeta, cls).__new__(cls, name, bases, attr_dict)

        # Initialise the manager to hook into this class (for now the manager
        # must be called 'objects')
        if name not in ('RiakBaseModel', 'RiakModel'):
            try:
                manager = attr_dict['objects']
            except KeyError, e:
                raise RiakModelMissingManager('Your model must have an objects manager')
            else:
                if not isinstance(manager, RiakManager):
                    raise RiakModelMissingManager('Objects is not a manager')
                # Make the manager use this class
                manager.set_model(model_class, name)

        return model_class

class RiakBaseModel(object):

    __metaclass__ = ModelMeta

    def __init__(self, *args, **kwargs):
        '''
        Gives each instance a copy of the fields defined at a class level and
        sets their internal values to those provided in kwargs/
        '''
        # Note that only base_fields remains at the class level
        self.fields = copy.deepcopy(self.base_fields)
        for k,v in self.fields.iteritems():
            # We do not force values to be available for fields right away
            v.value = kwargs.get(k, None)

    def __iter__(self):
        '''
        When looping, creates generator of fields
        '''
        for name in self.fields:
            yield self[name]

    def __getitem__(self, name):
        '''
        Allows key indexing 
        '''
        return self.fields[name]

    def __getattr__(self, name):
        '''
        If name does not exist in class, check the fields we have stored.
        '''
        try:
            return self.fields[name].value
        except KeyError:
            raise AttributeError('%s has not attribute %s' % (self.__class__.__name__,
                                                              name))

    def __setattr__(self, name, value):
        '''
        Sets values inside fields if name is a known field name.
        '''
        try:
            # Must use __dict__ explicitly in setattr!
            field = self.__dict__['fields'][name]
        except KeyError, e:
            super(RiakBaseModel, self).__setattr__(name, value)
        else:
            field.value = value

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
        key = self.generate_key()
        data = self.generate_values()
        bucket_name = self.bucket_name

        #Innitializing the client and the bucket name
        client = riak_client
        bucket = client.bucket(bucket_name)

        #Creating the bucket
        entry = bucket.new(key, data=data)
        #Saving it
        entry.store()

        

    def validate_fields(self, *args, **kwargs):
        '''
        Iterates over every field and ensures that if it is required that it
        has a legit value.
        '''
        for name, field in self.fields.iteritems():
            if not field.is_valid():
                raise AttributeError('%s field is required' % name)

    def generate_key(self, *args, **kwargs):
        '''
        Uses the key separator and and key order names to construct a key in
        the desired format.
        '''
        key_list = [str(self[attr].value) for attr in self.key_order if self[attr].in_key]
        self.key = self.key_seperator.join(key_list)
        return self.key

    def generate_values(self, *args, **kwargs):
        '''
        The dictionary that will be passed as a json like object in the
        key-value store.
        '''
        key_dict = dict([(name, field.value) for name, field in self.fields.iteritems() if field.in_value])
        self.values = key_dict
        return self.values


