from base import *


class Campaign(DbObject):
    @property
    @linked('offers')
    def offers(self):
        pass

    @property
    @linked('paused_offers')
    def paused_offers(self):
        pass

class Offer(DbObject):
    pass  # default implementation


class Conversion(DbObject):
    @property
    @time('time')
    def time(self):
        pass

    @property
    @money('revenue')
    def revenue(self):
        pass


class Hit(DbObject):
    @property
    @linked('campaign_id')
    def campaign(self):
        pass

    @property
    @linked('destination_id')
    def destination(self):
        pass

    @property
    @money('cost')
    def cost(self):
        pass

    @property
    @time('time')
    def time(self):
        pass


# see entity names in core/src/campaigns/model.rs
ENTITIES = {
    'Campaign': Campaign,
    'Offer': Offer,
    'Conversions': Conversion,
    'Hits': Hit
}



class DataimportScheme(object):
    _COLUMNS = (('name', str), ('_counter', long))

    def __init__(self):
        self.entities_info = {}

    def read(self, reporting_connection):
        c = reporting_connection.connected()
        for row, _, _ in c.read("SELECT * FROM dataimport_scheme;", columns=_COLUMNS):
            self.entities_info.update(row)
