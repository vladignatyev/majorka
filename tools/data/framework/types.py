from decimal import Decimal
from uuid import UUID
from datetime import datetime


class ModelTypes(object):
    STRING = 'String'
    BOOLEAN = 'UInt8'
    DECIMAL = 'Decimal'
    FLOAT = 'Float64'
    IDX = 'UInt64'
    INTEGER = 'Int64'
    ARRAY_OF_STRINGS = 'Array(String)'
    ARRAY_OF_IDX = 'Array(UInt64)'
    DATETIME = 'DateTime'
    DATE = 'Date'
    MONEY = 'Decimal'

def factory_from_db_type(db_type):
    return DB_TYPES.get(db_type, DB_TYPES['UNKNOWN'])

def factory_into_db_type(db_type):
    if type(db_type) is tuple or type(db_type) is list:
        return db_type[1]
    return INTO_DB_TYPES.get(db_type, INTO_DB_TYPES['UNKNOWN'])

def _db_type_bool(db_val):
    return int(db_val) == 1

def _db_type_date(db_val):
    return datetime.strptime(db_val, '%Y-%m-%d')

def _db_type_datetime(db_val):
    return datetime.strptime(db_val, '%Y-%m-%d %H:%M:%S')

def _db_type_array(t):
    def f(db_val):
        array_items_str = db_val[1:-1].split(',')
        items_without_dashes = map(lambda c: c[1:-1] if "'" in c else c, array_items_str)
        return map(lambda item: t(item), items_without_dashes)
    return f

DB_TYPES = {
    'UInt8': _db_type_bool,
    'UInt16': int,
    'UInt32': int,
    'UInt64': int,
    'Int8': int,
    'Int16': int,
    'Int32': int,
    'Int64': int,
    'Float32': Decimal,
    'Float64': Decimal,
    'Decimal': Decimal,
    'String': str,
    'UUID': UUID,
    'Enum8': str,
    'Enum16': str,
    'Array(String)':  _db_type_array(str),
    'Array(UInt8)':   _db_type_array(int),
    'Array(UInt16)':  _db_type_array(int),
    'Array(UInt32)':  _db_type_array(int),
    'Array(UInt64)':  _db_type_array(int),
    'Array(Int8)':    _db_type_array(int),
    'Array(Int16)':   _db_type_array(int),
    'Array(Int32)':   _db_type_array(int),
    'Array(Int64)':   _db_type_array(int),
    'Array(Float32)': _db_type_array(Decimal),
    'Array(Float64)': _db_type_array(Decimal),
    'Date': _db_type_date,
    'DateTime': _db_type_datetime,
    'UNKNOWN': str # todo
}

def _db_type_into_bool(obj_val):
    if obj_val:
        return 1
    else:
        return 0

def _db_type_into_int(obj_val):
    return obj_val

def _db_type_into_decimal(obj_val):
    if type(obj_val) is tuple:
        return str(Decimal(obj_val[0]))
    return str(Decimal(obj_val))

def _db_type_into_string(obj_val):
    return str(obj_val)

def _db_type_into_date(obj_val):
    return datetime.strftime(obj_val, '%Y-%m-%d')

def _db_type_into_datetime(obj_val):
    return datetime.strftime(obj_val, '%Y-%m-%d %H:%M:%S')

def _db_type_into_array(t):
    def f(obj_val):
        if len(obj_val) == 0:
            return '[]'                 # empty array, nothing to do
        if hasattr(obj_val[0], '_idx'):  # if array of linked objects
            # use value._idx as value, instead of str(value)
            return "[%s]" % (','.join(map(lambda v: str(v._idx), obj_val)))
        else:                           # if array of primitive types
            if type(obj_val[0]) is str or type(obj_val[0]) is unicode:
                return "['%s']" % ('\',\''.join(map(lambda v: str(v), obj_val)))
            else:
                return "[%s]" % (','.join(map(lambda v: str(v), obj_val)))
    return f

def _db_type_into_money(obj_val):
    value, currency = obj_val
    return str(Decimal(value))

def _db_type_into_linked(obj_val):
    return obj_val._idx

INTO_DB_TYPES = {
    'UInt8': _db_type_into_bool,
    'Money': _db_type_into_money,
    'UInt16': _db_type_into_int,
    'UInt32': _db_type_into_int,
    'UInt64': _db_type_into_int,
    'Int8': _db_type_into_int,
    'Int16': _db_type_into_int,
    'Int32': _db_type_into_int,
    'Int64': _db_type_into_int,
    'Float32': _db_type_into_decimal,
    'Float64': _db_type_into_decimal,
    'Decimal': _db_type_into_decimal,
    'String': _db_type_into_string,
    'UUID': _db_type_into_string,
    'Enum8': _db_type_into_string,
    'Enum16': _db_type_into_string,
    'Array(String)':  _db_type_into_array(str),
    'Array(UInt8)':   _db_type_into_array(int),
    'Array(UInt16)':  _db_type_into_array(int),
    'Array(UInt32)':  _db_type_into_array(int),
    'Array(UInt64)':  _db_type_into_array(int),
    'Array(Int8)':    _db_type_into_array(int),
    'Array(Int16)':   _db_type_into_array(int),
    'Array(Int32)':   _db_type_into_array(int),
    'Array(Int64)':   _db_type_into_array(int),
    'Array(Float32)': _db_type_into_array(Decimal),
    'Array(Float64)': _db_type_into_array(Decimal),
    'Date': _db_type_into_date,
    'DateTime': _db_type_into_datetime,
    'UNKNOWN': _db_type_into_string # todo
}
