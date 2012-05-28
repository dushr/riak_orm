from db import riak_client
from db import DoesNotExistError
from riak import key_filter

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

    def get(self, key):
        '''
        This method gets the key from riak 
        '''
        bucket = riak_client.bucket(self.model.bucket_name)
        result = bucket.get(key)
        try:
            return self.model(**result.get_data())
        except:
            raise DoesNotExistError('%s Does not exist in %s model' % (key, 
                                                                       self.model.__class__.__name__))


    def filter(self, **kwargs):
        '''
        This is method uses the key filter property of 
        riak returns the raw data from riak.
        '''
        if not set(kwargs).issubset(set(self.model.key_order)):
            raise ValueError('Invalid Filter query')
            ## it would be cool if we could tell which field didnt match
            ## TODO

        or_filters = dict([(k, kwargs[k]) for k in kwargs.iterkeys()])
        _list_of_orfilters = []
        for or_filter in or_filters.iteritems():
            _list_of_orfilters.append(self._make_orfilter(or_filter[0], or_filter[1]))

        filters = self._make_andfilters(_list_of_orfilters)
        print filters
        query = riak_client.add(self.model.bucket_name)
        query.add_key_filters(filters)
        _data = query.map(timeout=10000)
        return _data


    def _make_orfilter(self, filter, values):
        '''
        '''
        seperator = self.model.key_seperator
        key_index = self.model.key_order.index(filter) + 1
        tokenize_filter = key_filter.tokenize(seperator, key_index)

        for v in values:
            try:
                filters = filters | key_filter.eq(str(v))
            except NameError:
                filters = key_filter.eq(str(v))

        final = tokenize_filter + filters
        return final

    def _make_andfilters(self, orfilters):
        for f in orfilters:
            try:
                filters = filters & f
            except:
                filters = f
        return filters  






    
