from db import riak_client
from db import DoesNotExistError
from riak import key_filter
from hashlib import md5, sha1
import base64

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
        filter_set = [filt.split('__')[0] for filt in kwargs.iterkeys()]
        if not set(filter_set).issubset(set(self.model.key_order)):
            raise ValueError('Invalid Filter query')
            ## it would be cool if we could tell which field didnt match
            ## TODO

        #the or filter is assumed to be the field name, so we split it 
        #accross our identifier '__' and add to the or_filter 
        or_filters = dict([(k, kwargs.pop(k)) for k,v in kwargs.items() if k.find('__') == -1])
        _list_of_orfilters = []
        for or_filter in or_filters.iteritems():
            _list_of_orfilters.append(self._make_orfilter(or_filter[0], or_filter[1]))

        ## Now we will only have filters which are attr based
        range_filters = dict([(k.split('__')[0], kwargs.pop(k)) for k,v in kwargs.items() if k.split('__')[1] == 'range'])
        _list_of_range_filters = []
        for range_filter in range_filters.iteritems():
            _list_of_range_filters.append(self._make_rangefilter(range_filter[0], range_filter[1]))

        all_filters = _list_of_range_filters + _list_of_orfilters

        filters = self._make_andfilters(all_filters)
        query = riak_client.add(self.model.bucket_name)
        query.add_key_filters(filters)
        _data = query.run(timeout=10000)
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

    def _make_rangefilter(self, filter, values):

        seperator = self.model.key_seperator
        key_index = self.model.key_order.index(filter) + 1
        tokenize_filter = key_filter.tokenize(seperator, key_index)
        if not len(values) == 2:
            raise ValueError('Range Filter takes only two values')
        final = tokenize_filter + key_filter.between(values[0], values[1])
        return final


    def count(self, generate=False, **kwargs):
        materialized = getattr(self.model, 'materialized', None)
        generate = kwargs.pop('generate', False)
        if materialized:
            hashed_keys = self._get_query_hash(**kwargs)
            try:
                count = materialized.objects.get(hashed_keys.hash).value
            except:
                generate = True
            if generate:
                ## Materialized doesn't exist
                ## or generate is
                ## get the count from the filter
                count = len(self.filter(**kwargs))
                ## save it in the materialized model
                key = hashed_keys.hash
                base64key = hashed_keys.base64_hash
                m = materialized(key=key, base64key=base64key, value=count)
                m.save()

        else:
            count = len(self.filter(**kwargs))

        return count



    def _get_query_hash(self, hash_type='md5', do_base64 = True,  **kwargs):
        try:
            hash_type = kwargs.pop('hash_type')
        except KeyError:
            hash_type = hash_type
        try:
            do_base64 = kwargs.pop('do_base64')
        except KeyError:
            do_base64 = do_base64

        query_string = ''
        for key in sorted(kwargs.iterkeys()):
            query_string += (':'.join([str(key), str(kwargs[key])]) + '.')
        valid_hash_types = {
                        'md5': md5,
                        'sha1': sha1,
                    }

        hash_method = valid_hash_types.get(hash_type, None)
        if not hash_method:
            raise AttributeError('Not a valid hash type')
        # TODO: Error handling
        hash_string = hash_method(query_string).hexdigest()

        if do_base64:
            # TODO: Error handling
            base64_hash = base64.b64encode(hash_string)

        return_dict = {
            'hash': hash_string, 
        }
        # TODO: There has to be a better way to do this
        if base64_hash:
            return_dict.update({
                'base64_hash': base64_hash
                })
        return type('return_dict', (object,), return_dict)





    
