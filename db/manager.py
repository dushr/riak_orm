from db import riak_client
from db import DoesNotExistError
from riak import key_filter
from hashlib import md5, sha1
from itertools import groupby
import base64


class RiakQuerySet(object):
    '''
    QuerySet which contains riak model objects or a list of keys 
    pointing to that object in riak.
    If the filter is materialized (the query ran and stored the list
    of keys which statisfied the filter into a unique identifier)
    Then it returns a list of those keys.
    Else 
    if the query is not materialized, it returns the actual objects/maplinks
    '''
    def __init__(self, model, list_of_objects, list_of_keys=[]):
        '''
        initialize the queryset with the model and the objects/keys
        '''
        self.queryset = list_of_objects
        self.model = model
        self.key_queryset = list_of_keys

    def __iter__(self):
        '''
        making the queryset iterable.
        '''
        for obj in self.queryset:
            yield obj

    def generate_key_queryset(self):
        '''
        goes through every object in the qs and gets their keys.
        '''
        if not self.key_queryset:
            self.key_queryset = [q.get_key() for q in self.queryset]
        return self.key_queryset

    def sort_key_queryset(self, by=None):
        '''
        Sorts the keys in the queryset according to a field which is in
        key_order.
        '''
        if by:
            key_order = self.model.key_order
            if by not in key_order:
                raise ValueError('%s is not in the Key of the model' %by)
            # get the index of the parameter, will use it when spliting the
            # keys
            group_index = key_order.index(by)
            seperator = self.model.key_seperator
            # generate the key_queryset. If the key_querset exists it will
            # return that and not generate it.
            self.generate_key_queryset()
            # The sort key, used to sort the list of keys depending on the
            # parameter(by) split the key with the seperator and then get the
            # value we want from the index 
            sort_key = lambda x: x.split(seperator)[group_index]
            self.key_queryset = sorted(self.key_queryset, key=sort_key)

        return self.key_queryset


    def count(self, group_by=None):
        '''
        Returns the number of objects/keys in the querset.
        if you pass a group_by, the it groups the count with that parameter if
        it is present in the keyorder of the model.
        '''
        if group_by:
            # Sort the queryset according the group by param
            self.sort_key_queryset(by=group_by)
            key_order = self.model.key_order
            # get the index of the group_by param from the keyorder
            group_index = key_order.index(group_by)
            seperator = self.model.key_seperator
            # the function which returns the value on which the count should be
            # grouped.
            sort_key = lambda x: x.split(seperator)[group_index]
            count_list = [(q,len(list(g))) for q,g in groupby(self.key_queryset, key=sort_key)]
            return count_list
        return len(self.generate_key_queryset())

class RiakFilter(object):
    '''
    The filter class for the riak manager. it takes in the filter parameters
    and returns the queryset which satisfies those queries. 
    If the model has a materialized class defined, then it would cache the
    results of the query in the materialized class and will not run the whole
    key filter again. 
    we can force it to generate the queryset from the raw data again by passing
    a generate=True flag in our filter query.
    '''

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
        # get the range fitlers, that is mostly date
        range_filters = dict([(k.split('__')[0], kwargs.pop(k)) for k,v in kwargs.items() if k.split('__')[1] == 'range'])
        _list_of_range_filters = []
        for range_filter in range_filters.iteritems():
            _list_of_range_filters.append(self._make_rangefilter(range_filter[0], range_filter[1]))

        all_filters = _list_of_range_filters + _list_of_orfilters
        # now all the filters have to be made an and filter, becuase the query
        # has to satisfy all the filters
        filters = self._make_andfilters(all_filters)
        query = riak_client.add(self.model.bucket_name)
        query.add_key_filters(filters)
        # run the query, with a huge timeout
        _data = query.run(timeout=10000000)
        return _data


    def _make_orfilter(self, filter, values):
        '''
        it creates an all fitler depending on the param and the list of values.
        site = [1,2,3]
        site in 1 or 2 or 3
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
        '''
        this creates an and filter depending on the list of orfitlers
        site = [1.2.3] and category=[140,150]
        '''
        for f in orfilters:
            try:
                filters = filters & f
            except:
                filters = f
        return filters

    def _make_rangefilter(self, filter, values):
        '''
        makes a range filter depending on the filter name and the tuple of
        range values
        date = (20120525, 20120530)
        from 25th may 2012 to 30th may 2012
        '''
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
        '''
        Get the hash for the query, this first sorts the kwargs depending on
        the key and then returns the md5 hash for the query.
        it also returns the base64 hash of the query, which will help us decode
        the query later if we want to.
        We can change the hash_type from md5 to sha1 if required.
        '''
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
        '''
        Now if the materialzed model is defined for the model, the result of
        the queryset will be saved in the materialized model with the md5/sha1
        of the query as the key.
        '''
        materialized = getattr(self.model, 'materialized', None)
        if materialized:
            m = materialized()
            m.key = hashed_keys.hash
            m.base64key = hashed_keys.base64_hash
            m.value = [q.get_key() for q in list_of_objects]
            m.save()

    def __call__(self, generate=False, *args, **kwargs):
        '''
        if the materialized model is defined for the particular model, first
        check if the query as the key is stored in the materialized model and
        return the result from there, otherwise run the filter and save the
        stuff in the materialized model and return the objects.
        If the materiazlied model is not defined then run the filter as normal
        and return the queryset.
        '''
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
        # set the filter for the manager
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

    




    
