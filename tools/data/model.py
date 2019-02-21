class DbObject(object):
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

class Campaign(DbObject):
    pass

class Offer(DbObject):
    pass

class Conversion(DbObject):
    pass

class Hit(DbObject):
    pass

# see entity names in core/src/campaigns/model.rs
ENTITIES = {
    'Campaign': Campaign,
    'Offer': Offer,
    'Conversions': Conversion,
    'Hits': Hit
}
ENTITIES_KEYS = ENTITIES.keys()
