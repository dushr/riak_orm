from db import riak_client
from db import DoesNotExistError
from riak import key_filter
from hashlib import md5, sha1
from itertools import groupby
import base64


class RiakQuerySet(object):

    def __init__(self, model, list_of_objects, list_of_keys=[]):
        self.queryset = list_of_objects
        self.model = model
        self.key_queryset = list_of_keys

    def __iter__(self):
        for obj in self.queryset:
            yield obj

    def generate_key_queryset(self):
        if not self.key_queryset:
            self.key_queryset = [q.get_key() for q in self.queryset]
        return self.key_queryset

    def sort_key_queryset(self, by=None):
        if by:
            key_order = self.model.key_order
            if by not in key_order:
                raise ValueError('%s is not in the Key of the model' %by)
            group_index = key_order.index(by)
            seperator = self.model.key_seperator
            self.generate_key_queryset()
            sort_key = lambda x: x.split(seperator)[group_index]
            self.key_queryset = sorted(self.key_queryset, key=sort_key)

        return self.key_queryset


    def count(self, group_by=None):
        if group_by:
            self.sort_key_queryset(by=group_by)
            key_order = self.model.key_order
            group_index = key_order.index(group_by)
            seperator = self.model.key_seperator
            sort_key = lambda x: x.split(seperator)[group_index]
            count_list = [(q,len(list(g))) for q,g in groupby(self.key_queryset, key=sort_key)]
            return count_list
        return len(self.generate_key_queryset())

class RiakFilter(object):

    def __init__(self):
        self.model = None
        self.queryset = []

    def set_model(self, model_cls):
        self.model = model_cls

    def _filter(self, **kwargs):
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


    # def count(self, generate=False, **kwargs):
    #     materialized = getattr(self.model, 'materialized', None)
    #     if materialized:
    #         hashed_keys = self._get_query_hash(**kwargs)
    #         try:
    #             count = materialized.objects.get(hashed_keys.hash).value
    #         except DoesNotExistError:
    #             generate = True
    #         if generate:
    #             ## Materialized doesn't exist
    #             ## or generate is
    #             ## get the count from the filter
    #             count = len(self.filter(**kwargs))
    #             ## save it in the materialized model
    #             key = hashed_keys.hash
    #             base64key = hashed_keys.base64_hash
    #             m = materialized(key=key, base64key=base64key, value=count)
    #             m.save()

    #     else:
    #         count = len(self.filter(**kwargs))

    #     return count


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

    def generate_materialized(self, list_of_objects, hashed_keys):
        materialized = getattr(self.model, 'materialized', None)
        if materialized:
            m = materialized()
            m.key = hashed_keys.hash
            m.base64key = hashed_keys.base64_hash
            m.value = [q.get_key() for q in list_of_objects]
            m.save()

    def __call__(self, generate=False, *args, **kwargs):
        materialized = getattr(self.model, 'materialized', None)
        if materialized:
            hashed_keys = self._get_query_hash(**kwargs)
            try:
                list_of_keys = materialized.objects.get(hashed_keys.hash).value
                queryset = RiakQuerySet(self.model, [], list_of_keys=list_of_keys)
                if not isinstance(list_of_keys, list):
                    generate = True
            except DoesNotExistError:
                generate = True
            if generate:
                list_of_objects = self._filter(*args, **kwargs)
                self.generate_materialized(list_of_objects, hashed_keys)
            else:
                return queryset
        else:
            list_of_objects = self._filter(*args, **kwargs)
        
        return RiakQuerySet(self.model, list_of_objects)




class RiakManager(object):
    def __init__(self):
        self.model = None
        self.filter = RiakFilter()

    def set_model(self, model_cls, name):
        '''
        This method is called in ModelMeta and sets the manager to be used for
        the given model. It also sets the a class variable for the bucket name
        based on the model name
        '''
        self.model = model_cls
        # For now, we simply use the name of the class lower cased
        self.model.bucket_name = name.lower()
        self.filter.set_model(model_cls)

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

    




    
