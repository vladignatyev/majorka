from datetime import datetime
from decimal import *
from functools import wraps

from types import ModelTypes, factory_into_db_type


class ReportingObject(object):
    # todo: extract class ReportingDataObject
    ''' Default implementation '''
    def into_db_columns(self):
        obj_keys = sorted(filter(lambda k: k != 'id', self.__dict__.keys()))
        public_self_attrs = filter(lambda a: not a.startswith('_'), obj_keys)
        return [('id', ModelTypes.INTEGER)] + map(lambda k: (k, ModelTypes.STRING), public_self_attrs)

    def into_db_row(self, required_columns=None):
        if required_columns is None:
            db_columns = self.into_db_columns()
        else:
            db_columns = required_columns

        db_row = [None] * len(db_columns)
        for i, (name, db_type) in enumerate(db_columns):
            raw_val = getattr(self, name)
            db_row[i] = (name, factory_into_db_type(db_type)(raw_val))

        return dict(db_row)
    

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
