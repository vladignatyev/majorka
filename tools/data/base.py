from datetime import datetime
from decimal import *
from functools import wraps


class DataObject(object):
    MONEY_DECIMAL_SHIFT = 100000  # see core/src/campaigns/currency/mod.rs

    def __init__(self, connection, id, **kwargs):
        self._from_id(id)
        self._connection = connection
        self.__dict__.update(**kwargs)

    def _from_id(self, id):
        self._id = id
        self._entity = id.split(':')[0]
        self._idx = long(id.split('[')[1].split(']')[0])


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
