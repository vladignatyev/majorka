from base import DbObject


class Campaign(DbObject):
    @property
    def alias(self):
        return self.__dict__['alias']

    @property
    def offers(self):
        self._getter_stub()

    def get_offers(self, connection):
        return self._get_linked_objects(connection, 'offers')

    @property
    def paused_offers(self):
        self._getter_stub()

    def get_paused_offers(self, connection):
        return self._get_linked_objects(connection, 'paused_offers')


class Offer(DbObject):
    pass  # default implementation


class Conversion(DbObject):
    @property
    def time(self):
        return self._getter_timefield()

    @property
    def revenue(self):
        return self._getter_money_field('revenue')


class Hit(DbObject):
    def get_campaign(self, connection):
        return self._get_linked_objects(connection, 'campaign_id')

    def get_destination(self, connection):
        return self._get_linked_objects(connection, 'destination_id')

    @property
    def cost(self):
        return self._getter_money_field('cost')

    @property
    def time(self):
        return self._getter_timefield()


# see entity names in core/src/campaigns/model.rs
ENTITIES = {
    'Campaign': Campaign,
    'Offer': Offer,
    'Conversions': Conversion,
    'Hits': Hit
}
