from db.manager import RiakManager
from db.fields import RiakField
from db.models import RiakModel



class NewDataMaterialized(RiakModel):
    key = RiakField()
    base64key = RiakField(in_key=False, required=False)
    value = RiakField(in_key=False)

    key_order = ('key',)
    key_seperator = ':'
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

class New(RiakModel):
    l = RiakField()
    m = RiakField()
    cat = RiakManager()