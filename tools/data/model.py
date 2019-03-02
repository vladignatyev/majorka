from framework.base import *
from framework.reporting import *
from framework.types import *


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
        [('name', Type.String()),
         ('alias', Type.String()),

         ('offers', Type.Array(items=Type.Idx())),
         ('paused_offers', Type.Array(items=Type.Idx())),

         ('optimize', Type.Bool()),
         ('optimization_paused', Type.Bool()),
         ('hit_limit_for_optimization', Type.Int32()),
         ('slicing_attrs', Type.Array(items=Type.String()))]



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
        [('external_id', Type.String()),
         ('revenue', Type.Money()),
         ('status', Type.String()),
         ('time', Type.DateTime())]


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
        [('campaign', (Type.Idx(), _db_type_into_linked)),
         ('destination', (Type.Idx(), _db_type_into_linked)),
         ('cost', Type.Money()),
         ('time', Type.DateTime())] + \
         sorted(map(lambda dim: ("dim_%s" % dim, Type.String()) , self.__dict__['dimensions'].keys()))

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



class DataImport(object):
    def __init__(self, bus, report_db):
        self.bus = bus
        self.reporting = report_db
        # todo:
        self.reporting.drop()

    def get_entity_last_idx(self, entity_cls):
        db_name = self.reporting.name
        table_name = entity_cls.TABLE_NAME
        sql = "SELECT MAX(id) AS last_idx FROM {db}.{table}".format(db=db_name, table=table_name)

        try:
            for o, i, l in self.reporting.connected().read(sql=sql, columns=(('last_idx', 'UInt64'))):
                return o['last_idx']
        except DbError as e:
            return 0

    def init_entity(self, entity_name):
        entity = ENTITIES[entity_name]
        result = self.reporting.connected().write(sql=self.reporting.sql.create_table_for_reporting_object(entity))
        if not result:
            raise Exception("Unable to initialize entity table.")

    def load_entity(self, entity_name):
        entity = ENTITIES[entity_name]
        last_id = self.get_entity_last_idx(entity)
        if last_id == 0:
            self.init_entity(entity_name)

        return list(self.bus.multiread(entity_name, start=last_id))

    def import_entity(self, entity_name, entity_objs):
        if len(entity_objs) == 0:
            print "All `%s` are already imported! Skipping." % entity_name  # todo: log
            return

        entity = ENTITIES[entity_name]
        columns = entity.into_db_columns()
        column_names = zip(*columns)[0]
        objects_as_db_values = map(lambda o: o.into_db_values(columns=columns), entity_objs)
        sql = self.reporting.sql.insert_values(table=entity.TABLE_NAME,
                                               values=objects_as_db_values,
                                               columns=column_names)
        result = self.reporting.connected().write(sql=sql)
        if not result:
            raise Exception("Unable to import entity `{entity_name}`".format(entity_name=entity_name))

    def load_simple_entities(self):
        simple = ['Campaign', 'Offer', 'Conversions']
        for entity_name in simple:
            print "Importing entity %s" % entity_name # todo add logging
            objects_to_import = self.load_entity(entity_name)
            self.import_entity(entity_name=entity_name, entity_objs=objects_to_import)
