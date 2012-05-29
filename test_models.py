from db.manager import RiakManager
from db.fields import RiakField
from db.models import RiakModel



class TestDataModel(RiakModel):
    category = RiakField()
    site = RiakField()
    language = RiakField()
    datetime = RiakField()
    item_id = RiakField(in_key=False, required=False)

    key_order = ('datetime','site','category', 'language')
    key_seperator = '#'


    objects = RiakManager()


class MaterializedModel(RiakModel):
    key = RiakModel()
    base64key = RiakModel(in_key=False, required=False)
    value = RiakModel(in_key=False)

    key_order = ('key')
    key_seperator = ':'

    objects = RiakManager()


class NewDataMaterialized(MaterializedModel):
    objects = RiakManager()


class NewDataModel(RiakModel):
    category = RiakField()
    site = RiakField()
    language = RiakField()
    date = RiakField()
    time = RiakField()
    item_id = RiakField(in_key=False, required=False)

    key_order = ('date','time','site','category', 'language')
    key_seperator = ':'

    objects = RiakManager()

    materialized = NewDataMaterialized