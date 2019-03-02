from datetime import datetime
from decimal import *
from functools import wraps

from types import *


class ReportingObject(object):
    ''' Default implementation '''
    TABLE_NAME = None

    INDEX = ('id', 'date_added',)
    @classmethod
    def default_columns(cls):
        return [('id', Type.UInt64()), ('date_added', Type.Date())]

    @classmethod
    def into_db_columns(cls):
        default_cols = cls.default_columns()
        default_cols_names = dict(cls.default_columns()).keys()
        obj_keys = sorted(filter(lambda k: k not in default_cols_names, cls.__dict__.keys()))
        public_self_attrs = filter(lambda a: not a.startswith('_'), obj_keys)
        return default_cols + map(lambda k: (k, Type.String()), public_self_attrs)

    @property
    def date_added(self):
        return datetime.now()

    def into_db_row(self):
        db_columns = self.into_db_columns()
        db_row = [None] * len(db_columns)
        for i, (name, db_type) in enumerate(db_columns):
            if hasattr(self, name):
                raw_val = getattr(self, name)
                db_row[i] = (name, db_type.into_db_value(py_value=raw_val, column_name=name))
            else:
                db_row[i] = (name, db_type.into_db_value(py_value=None, column_name=name))

        return dict(db_row)

    def into_db_values(self, columns):
        if type(columns[0]) is list or type(columns[0]) is tuple:
            column_names = zip(*columns)[0]
        else:
            column_names = columns

        db_row = self.into_db_row()

        values = [None] * len(column_names)
        for i, k in enumerate(column_names):
            values[i] = db_row.get(k, columns[i][1].into_db_value(columns[i][1].default_py_value(), column_name=column_names[i]))
        return values

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)


class DataObject(object):
    MONEY_DECIMAL_SHIFT = 100000  # see core/src/campaigns/currency/mod.rs

    def __init__(self, bus, id, **kwargs):
        self._from_id(id)
        self._connection = bus
        self.__dict__.update(**kwargs)

    def _entity_id_to_idx(self, entity_id):
        return int(entity_id.split('[')[1].split(']')[0])

    def _from_id(self, id):
        self._id = id
        self._entity = id.split(':')[0]
        self._idx = self.id = self._entity_id_to_idx(id)


def money(field):
    def wrapper(f):
        @wraps(f)
        def wrapped(self):
            value = Decimal(self.__dict__[field]['value']) / Decimal(self.MONEY_DECIMAL_SHIFT)
            currency = self.__dict__[field]['currency']

            return (value, currency)
        return wrapped
    return wrapper

def time(field):
    def wrapper(f):
        @wraps(f)
        def wrapped(self):
            return datetime.utcfromtimestamp(int(self.__dict__[field]['secs_since_epoch']))
        return wrapped
    return wrapper

def linked(ids_field):
    def wrapper(f):
        @wraps(f)
        def wrapped(self):
            pipe = self._connection.readonly()
            id_or_ids = self.__dict__[ids_field]
            if id_or_ids.__class__ is list:
                for linked_obj_id in id_or_ids:
                    pipe.by_id(linked_obj_id)
                return pipe.execute()
            else:
                some_id = id_or_ids
                pipe.by_id(some_id)
                return pipe.execute()[0]

        return wrapped
    return wrapper
