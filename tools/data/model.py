import logging

from framework.base import *
from framework.reporting import *
from framework.types import *
from framework.utils import diff, diff_apply


REPORTING_DB = 'majorka'


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
    class Dimension(Typecast):
        def _dimension_name_from_column_name(self, column_name):
            return column_name.split("dim_")[1]
        def into_db_value(self, context=None, py_value=None, column_name=None):
            dim_name = self._dimension_name_from_column_name(column_name)
            return u"{v}".format(v=unicode(context.__dict__['dimensions'].get(dim_name, "")))
        def into_db_type(self): return 'String'  # todo: typed dimensions
        def from_db_value(self, db_value, column_name=None):
            return str(db_value)
        def default_py_value(self): return ''
        def default_db_value(self): return None

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
        return self.static_columns() + \
         list(map(lambda dim: ("dim_%s" % dim, Hit.Dimension()) , self.__dict__['dimensions'].keys()))

    @classmethod
    def static_columns(cls):
        return cls.default_columns() + \
        [('campaign', Type.Idx()),
         ('destination', Type.Idx()),
         ('cost', Type.Money()),
         ('time', Type.DateTime())]


# see entity names in core/src/campaigns/model.rs
ENTITIES = {
    'Campaign': Campaign,
    'Offer': Offer,
    'Conversions': Conversion,
    'Hits': Hit
}


# _custom_diff_sorting maintains a stable order to keep 'diff' working and to build correct schema update
def _custom_diff_sorting(comparable_columns):

    static_columns_names = map(lambda col: col[0], Hit.static_columns())
    l = list(comparable_columns)
    l2 = sorted(list(filter(lambda col: not (col.name in static_columns_names), l)))

    comparable_static_columns = {}

    static_columns = filter(lambda col: col.name in static_columns_names, l)

    # don't know why, but static_columns are randomly sorted now,
    # so we just map them back into order provided by Hit.static_columns()
    for static_col in static_columns:
        comparable_static_columns[static_col.name] = static_col
    static_columns = [None] * len(static_columns)
    for i, name in enumerate(static_columns_names):
        static_columns[i] = comparable_static_columns[name]

    result = static_columns + l2
    return result


# TODO: 1) we already have ColumnsDef class.
#          ComparableColumn should be used in place of stupid (name, type)
#          tuples inside framework internals to avoid a messy castings,
#          checkings and such ugly stuff.
#          Furthemore, it's necessary for implementing migrations and computing
#          diffs for schema.
#       2) dynamic objects is well-defineable model, since we have a `context`
#          for every `into_db_value` call, so probably extract something into
#          base framework classes, i.e. calculating columns diff
#          and automigrating
class ComparableColumn(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __eq__(self, other):
        return self.name == other.name # and type(self.type) is type(other.type)

    def __hash__(self):
        return self.name.__hash__()

    def __cmp__(self, other):
        if other is None: return 1
        if self.name < other.name: return -1
        if self.name > other.name: return 1
        if self.name == other.name: return 0

    def __repr__(self):
        return "{name}".format(name=self.name)


def wrap_comparable(columns):
    return [ComparableColumn(name=name, type=type) for name, type in columns]

def unwrap_comparable_into_raw_columns(comparable):
    return [(c.name, c.type) for c in comparable]

def safe_dynamic_fields(columns):
    return [(c[0], Hit.Dimension() if 'dim_' in c[0] else c[1]) for c in columns]

class DataImport(object):
    LOGGER = 'dataimport'

    def __init__(self, bus, report_db, logger=logging.getLogger(LOGGER)):
        self.log = logger
        self.bus = bus
        self.reporting = report_db
        # report_db.drop()  # todo: remove after making tests

    def get_entity_last_idx(self, name, entity):
        db_name = self.reporting.name
        table_name = entity.TABLE_NAME

        sql = "SELECT MAX(id) AS last_idx, COUNT(*) AS count FROM {db}.{table}".format(db=db_name, table=table_name)

        try:
            for o, i, l in self.reporting.connected().read(sql=sql, columns=(('last_idx', 'UInt64'), ('count', 'UInt64'))):
                self.log.info("We have {count} `{entities}` in reporting storage. The last one has index id={id}".format(
                    id=o['last_idx'],
                    count=o['count'],
                    entities=name
                ))
                return o['last_idx']
        except DbError as e:
            return 0

    def init_entity(self, name, entity):
        result = self.reporting.connected().write(sql=self.reporting.sql.create_table_for_reporting_object(entity))
        if not result:
            raise Exception("Unable to initialize entity table for `{entity}`.".format(entity=name))

    def load_entity(self, name, entity):
        last_id = self.get_entity_last_idx(name, entity)
        if last_id == 0:
            self.init_entity(name, entity)

        return list(self.bus.multiread(name, start=last_id))
        # return list(self.bus.multiread(name, start=last_id, end=2)) # todo: remove after making tests

    def import_entity(self, name, table_name, objs, columns):
        column_names = zip(*columns)[0]
        objects_as_db_values = map(lambda o: o.into_db_values(columns=columns), objs)
        sql = self.reporting.sql.insert_values(table=table_name,
                                               values=objects_as_db_values,
                                               columns=column_names)

        result = self.reporting.connected().write(sql=sql)
        if not result:
            raise Exception("Unable to import entity `{name}`".format(name=name))
        self.log.info("Entity `{name}` has been imported succesfully".format(name=name))

    def load_simple_entities(self, entities=('Campaign', 'Offer', 'Conversions')):
        for name in entities:
            self.log.info("Importing entity {name}".format(name=name)) # todo add logging

            entity = ENTITIES[name]
            objects_to_import = self.load_entity(name, entity)

            if not self.do_we_need_to_import(name, objects_to_import):
                continue

            self.import_entity(name=name, table_name=entity.TABLE_NAME,
                               objs=objects_to_import, columns=objects_to_import[0].into_db_columns())

    def do_we_need_to_import(self, name, objs):
        if len(objs) > 0:
            return True

        self.log.info("All `{name}` are already imported! Skipping.".format(name=name))  # todo: log
        return False

    def load_hits(self):
        self.log.info("Loading hits...")

        name = 'Hits'
        entity = ENTITIES[name]
        table_name = entity.TABLE_NAME

        objects_to_import = self.load_entity(name=name, entity=entity)

        if not self.do_we_need_to_import(name, objects_to_import):
            return

        self.log.info("Calculating migration...")

        # todo: we need one mutable and one immutable variables for computing diff
        existing_columns = wrap_comparable(safe_dynamic_fields(self.reporting.connected().describe(table_name)))
        source_columns = wrap_comparable(safe_dynamic_fields(self.reporting.connected().describe(table_name)))

        for o in objects_to_import:
            delta = diff(existing_columns, wrap_comparable(o.into_db_columns()), custom_sorted=_custom_diff_sorting)
            if delta is not None:
                existing_columns = list(diff_apply(existing_columns, delta))

        new_after_list = diff(source_columns, existing_columns, custom_sorted=_custom_diff_sorting)

        if new_after_list is not None and len(new_after_list) > 0:
            self.log.info("We need to add {count} more columns.".format(count=len(new_after_list)))
            self.log.info("Samples:")
            for i in range(0, len(new_after_list) if len(new_after_list) < 3 else 3):
                self.log.info("\t%s: %s" % (new_after_list[i][0].name, new_after_list[i][0].type))
            if len(new_after_list) > 3:
                self.log.info("\t...")
            self.log.info("Applying new scheme")

            for new, after in new_after_list:
                sql = "ALTER TABLE {db}.{table} ADD COLUMN {new_name} {new_type} AFTER {after_name};".format(
                    db=self.reporting.name,
                    table=table_name,
                    new_name=new.name,
                    new_type=new.type.into_db_type(),
                    after_name=after.name)

                self.log.info("\t Creating a column `{column}` with type `{type}` after `{after}`".format(
                    column=new.name,
                    type=new.type.into_db_type(),
                    after=after.name
                ))

                result = self.reporting.connected().write(sql)
                if not result:
                    raise Exception("Error has occured while applying scheme.")
        else:
            self.log.info("\t We don't need any migrations! Just loading objects into storage.")

        columns = safe_dynamic_fields(self.reporting.connected().describe(table_name))

        self.import_entity(name=name, table_name=table_name,
                           objs=objects_to_import, columns=columns)

        self.log.info("Successfully imported {count} new hits into table `{table_name}`".format(
            count=len(objects_to_import),
            table_name=table_name
        ))
