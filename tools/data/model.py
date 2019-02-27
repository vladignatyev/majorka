from framework.base import *
from framework.types import ModelTypes, _db_type_into_money, _db_type_into_linked, _db_type_into_datetime


class Campaign(DataObject, ReportingObject):
    TABLE_NAME = 'campaigns'

    @property
    @linked('offers')
    def offers(self):
        pass

    @property
    @linked('paused_offers')
    def paused_offers(self):
        pass

    @classmethod
    def into_db_columns(cls):
        return cls.default_columns() + \
        [('name', ModelTypes.STRING),
         ('alias', ModelTypes.STRING),

         ('offers', ModelTypes.ARRAY_OF_IDX),
         ('paused_offers', ModelTypes.ARRAY_OF_IDX),

         ('optimize', ModelTypes.BOOLEAN),
         ('optimization_paused', ModelTypes.BOOLEAN),
         ('hit_limit_for_optimization', ModelTypes.INTEGER),
         ('slicing_attrs', ModelTypes.ARRAY_OF_STRINGS)]



class Offer(DataObject, ReportingObject):
    TABLE_NAME = 'offers'


class Conversion(DataObject, ReportingObject):
    TABLE_NAME = 'conversions'

    @property
    @time('time')
    def time(self):
        pass

    @property
    @money('revenue')
    def revenue(self):
        pass

    @classmethod
    def into_db_columns(cls):
        return cls.default_columns() + \
        [('external_id', ModelTypes.STRING),
         ('revenue', (ModelTypes.MONEY, _db_type_into_money)),
         ('status', ModelTypes.STRING),
         ('time', ModelTypes.DATETIME)]


class Hit(DataObject, ReportingObject):
    TABLE_NAME = 'hits'

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

    @property
    def external_id(self):
        return self.__dict__.get('external_id')

    def into_db_columns(self):
        return self.default_columns() + \
        [('campaign', (ModelTypes.IDX, _db_type_into_linked)),
         ('destination', (ModelTypes.IDX, _db_type_into_linked)),
         ('cost', (ModelTypes.MONEY, _db_type_into_money)),
         ('time', ModelTypes.DATETIME)] + \
         sorted(map(lambda dim: ("dim_%s" % dim, ModelTypes.STRING) , self.__dict__['dimensions'].keys()))

    def into_db_row(self):
        required_columns = self.into_db_columns()

        fields = dict(
            [('id', self.id),
             ('campaign', self._entity_id_to_idx(self.__dict__['campaign_id'])),
             ('destination', self._entity_id_to_idx(self.__dict__['destination_id'])),
             ('cost', _db_type_into_money(self.cost)), # only value of currency
             ('time', _db_type_into_datetime(self.time))]
        )

        if not all([field_name in dict(required_columns).keys() for field_name in fields.keys()]):
            raise Exception('`required_columns` should be greater or equal to `base_fields`')

        for column_name, type_def in required_columns:
            if column_name not in fields: # dimensions or something unknown
                if column_name.startswith('dim_'):
                    dimension_name = column_name.split('dim_')[1]
                    fields[column_name] = str(self.__dict__['dimensions'].get(dimension_name, ''))
                else:
                    fields[column_name] = ''

        return fields


# see entity names in core/src/campaigns/model.rs
ENTITIES = {
    'Campaign': Campaign,
    'Offer': Offer,
    'Conversions': Conversion,
    'Hits': Hit
}
