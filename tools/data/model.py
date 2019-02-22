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
