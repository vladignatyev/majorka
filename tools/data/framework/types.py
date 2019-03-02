from decimal import Decimal
from uuid import UUID
from datetime import datetime
from ipaddr import IPAddress
import random


class Typecast(object):
    def __init__(self):
        pass

    def into_db_value(self, context=None, py_value=None, column_name=None):
        return None

    def into_db_type(self):
        return None

    def from_db_value(self, db_value, column_name=None):
        return None

    def default_py_value(self):
        return None

    def default_db_value(self):
        return None

    def __repr__(self):
        return self.into_db_type()

    def __str__(self):
        return self.into_db_type()

class Type(object):
    class Undefined(Typecast): pass

    class Default(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return u"{v}".format(v=unicode(py_value))
        def into_db_type(self): return 'String'
        def from_db_value(self, db_value, column_name=None):
            return str(db_value)
            # return str(db_value.split("'")[1])
        def default_py_value(self): return ''
        def default_db_value(self): return None

    class UUID(Default):
        def __init__(self): self.value = UUID(int=random.getrandbits(128))
        def from_db_value(self, db_value, column_name=None): return UUID(db_value)
        def into_db_value(self, context=None, py_value=None, column_name=None): return str(py_value)
        def default_py_value(self): return self.value
        def into_db_type(self): return 'UUID'

    class Bool(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            if py_value is True:
                return 1
            else:
                return 0

        def into_db_type(self): return 'UInt8'
        def from_db_value(self, db_value, column_name=None):
            if int(db_value) == 1:
                return True
            else:
                return False

        def default_py_value(self): return False
        def default_db_value(self): return None

    class Integer(Typecast):
        def __init__(self, bits=64, unsigned=False):
            assert bits in [8, 16, 32, 64]
            self.bits = bits
            self.unsigned = unsigned

        def into_db_value(self, context=None, py_value=None, column_name=None):
            return '{val}'.format(val=int(py_value))

        def into_db_type(self):
            if self.unsigned:
                return 'UInt{b}'.format(b=self.bits)
            else:
                return 'Int{b}'.format(b=self.bits)

        def from_db_value(self, db_value, column_name=None):
            if db_value == '':
                return None
            return int(db_value)

        def default_py_value(self): return 0
        def default_db_value(self): return None

    class Idx(Integer):
        @classmethod
        def into_db_value(self, context=None, py_value=None, column_name=None):
            if type(py_value) is int:
                _idx = py_value
            elif hasattr(py_value, '_idx'):
                _idx = getattr(py_value, '_idx')
            else:
                raise Exception("Improper object passed.")
            return '{val}'.format(val=int(_idx))

        @classmethod
        def into_db_type(self):
            return 'UInt64'
        @classmethod
        def from_db_value(self, db_value, column_name=None):
            return int(db_value)
        @classmethod
        def default_py_value(self): return 0  # need a safe value
        @classmethod
        def default_db_value(self): return None


    class UInt64(Integer):
        def __init__(self):
            super(Type.UInt64, self).__init__(bits=64, unsigned=True)

    class UInt32(Integer):
        def __init__(self):
            super(Type.UInt32, self).__init__(bits=32, unsigned=True)

    class UInt16(Integer):
        def __init__(self):
            super(Type.UInt16, self).__init__(bits=16, unsigned=True)

    class UInt8(Integer):
        def __init__(self):
            super(Type.UInt8, self).__init__(bits=8, unsigned=True)

    class Int64(Integer):
        def __init__(self):
            super(Type.Int64, self).__init__(bits=64, unsigned=False)

    class Int32(Integer):
        def __init__(self):
            super(Type.Int32, self).__init__(bits=32, unsigned=False)

    class Int16(Integer):
        def __init__(self):
            super(Type.Int16, self).__init__(bits=16, unsigned=False)

    class Int8(Integer):
        def __init__(self):
            super(Type.Int8, self).__init__(bits=8, unsigned=False)


    class String(Default):
        def into_db_type(self): return 'String'
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return u"{v}".format(v=unicode(py_value))
        def from_db_value(self, db_value, column_name=None):
            return str(db_value)

    class Enum8(Default):
        def into_db_type(self): return 'Enum8'
    class Enum16(Default):
        def into_db_type(self): return 'Enum16'

    class Array(Typecast):
        def __init__(self, items):
            self._items_type = items

        def into_db_value(self, context=None, py_value=None, column_name=None):
            if type(py_value) is not list and type(py_value) is not tuple:
                raise Exception("Improper `py_value` passed. \n"
                                "Expected list or tuple, got `{t}`".format(t=type(py_value)))
            if len(py_value) == 0:
                return '[]'
            iter_func = lambda item: self._items_type.into_db_value(py_value=item, context=context, column_name=column_name)
            if type(self._items_type) is Type.String:
                return "['{v}']".format(v=','.join(map(iter_func, py_value)))
            else:
                return "[{v}]".format(v=','.join(map(iter_func, py_value)))

        def into_db_type(self):
            return 'Array({items_type})'.format(items_type=self._items_type.into_db_type())

        def from_db_value(self, db_value, column_name=None):
            return self.__class__.from_db_value_with_item_type(db_value, self._items_type)

        @classmethod
        def from_db_value_with_item_type(cls, db_value, item_type):
            if db_value == '[]':
                return []
            array_items_str = db_value[1:-1].split(',')
            if type(item_type) is type(Type.String()):
                item_func = lambda item: item_type.from_db_value(item[1:-1])
            else:
                item_func = lambda item: item_type.from_db_value(item)
            return map(item_func, array_items_str)

        def default_py_value(self):
            return []

        def default_db_value(self):
            return None

    class LinkedObjects(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            if (type(py_value) is not list) and (type(py_value) is not tuple):
                raise Exception("Invalid value.")
            if len(py_value) == 0:
                return '[]'

            if type(py_value[0]) is not int:
                iter_func = lambda item: Idx.into_db_type(item)
            elif type(py_value[0]) is int:
                iter_func = lambda item: '{v}'.format(v=int(item))
            else:
                raise Exception("Improper type of `py_value`.")
            return ','.join(map(iter_func, py_value))

        def into_db_type(self): return 'Array(UInt64)'
        def from_db_value(self, db_value, column_name=None):
            return Array.from_db_value_with_item_type(db_value, item_type=Idx)

    class Decimal(Typecast):
        def __init__(self, precision=10, scale=18):
            assert precision >= 1 and precision <= 38
            assert scale >= 0 and scale <= precision
            self.precision = precision
            self.scale = scale
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return "{v}".format(v=str(py_value))
        def into_db_type(self):
            return 'Decimal({p},{s})'.format(p=self.precision, s=self.scale)
        def from_db_value(self, db_value, column_name=None): return Decimal(db_value)
        def default_py_value(self): return Decimal(0.0)
        def default_db_value(self): return None

        @classmethod
        def from_db_type(cls, db_type):
            assert 'Decimal' in db_type
            precision, scale = map(int, db_type.split('Decimal(')[1].split(')')[0].split(','))
            if precision == 9:
                return Decimal32(scale=scale)
            elif precision == 18:
                return Decimal64(scale=scale)
            elif precision == 38:
                return Decimal128(scale=scale)
            else:
                return Decimal(precision=precision, scale=scale)

    class Decimal32(Decimal):
        def __init__(self, scale=4): super(Type.Decimal32, self).__init__(precision=9, scale=scale)
        def into_db_type(self): return 'Decimal32({s})'.format(s=self.scale)

    class Decimal64(Decimal):
        def __init__(self, scale=9): super(Type.Decimal64, self).__init__(precision=18, scale=scale)
        def into_db_type(self): return 'Decimal64({s})'.format(s=self.scale)

    class Decimal128(Decimal):
        def __init__(self, scale=19): super(Type.Decimal128, self).__init__(precision=38, scale=scale)
        def into_db_type(self): return 'Decimal128({s})'.format(s=self.scale)

    class Money(Decimal64):
        def __init__(self): super(Type.Decimal64, self).__init__(scale=5)

        def into_db_value(self, context=None, py_value=None, column_name=None):
            value, currency = py_value or self.default_py_value()
            return super(Type.Decimal64, self).into_db_value(py_value=value, context=context)
        def from_db_value(self, db_value, column_name=None):
            return (super(Type.Decimal64, self).from_db_value(value), 'Unknown')
        def default_py_value(self): return (Decimal(0.0), 'Unknown')

    class Float32(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return "{v}".format(v=str(py_value))
        def into_db_type(self): return 'Float32'
        def from_db_value(self, db_value, column_name=None): return float(db_value)
        def default_py_value(self): return 0.0
        def default_db_value(self): return None

    class Float64(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return "{v}".format(v=str(py_value))
        def into_db_type(self): return 'Float64'
        def from_db_value(self, db_value, column_name=None): return float(db_value)
        def default_py_value(self): return 0.0
        def default_db_value(self): return None

    class IPAddress(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            return "{v}".format(v=str(py_value))
        def into_db_type(self): return 'String'
        def from_db_value(self, db_value, column_name=None):
            return IPAddress(db_value)
        def default_py_value(self): return IPAddress('127.0.0.1')
        def default_db_value(self): return None

    class Date(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            value = py_value or self.default_py_value
            assert type(py_value) is datetime
            return datetime.strftime(value, '%Y-%m-%d')

        def into_db_type(self): return 'Date'
        def from_db_value(self, db_value, column_name=None):
            return datetime.strptime(db_value, '%Y-%m-%d')

        def default_py_value(self):
            now = datetime.now()
            return datetime(year=now.year, month=now.month, day=now.day)

        def default_db_value(self):
            return None

    class DateTime(Typecast):
        def into_db_value(self, context=None, py_value=None, column_name=None):
            value = py_value or self.default_py_value()
            assert type(py_value) is datetime
            return datetime.strftime(py_value, '%Y-%m-%d %H:%M:%S')

        def into_db_type(self): return 'DateTime'
        def from_db_value(self, db_value, column_name=None):
            return datetime.strptime(db_value, '%Y-%m-%d %H:%M:%S')

        def default_py_value(self):
            return datetime.now()

        def default_db_value(self):
            return None


def factory_from_db_type(db_type):
    known_type = KNOWN_DB_TYPES.get(db_type, None)
    if known_type is not None:
        return known_type
    else:
        return supported_type(db_type)

def supported_type(db_type):
    if 'Array' in db_type:
        type_param = db_type.split('Array(')[1].split(')')[0]
        return Type.Array(items=factory_from_db_type(type_param))
    elif 'Decimal32' in db_type:
        scale = int(db_type.split('Decimal32(')[1].split(')')[0])
        return Type.Decimal32(scale)

    elif 'Decimal64' in db_type:
        scale = int(db_type.split('Decimal64(')[1].split(')')[0])
        return Type.Decimal64(scale)

    elif 'Decimal128' in db_type:
        scale = int(db_type.split('Decimal128(')[1].split(')')[0])
        return Type.Decimal128(scale)

    elif 'Decimal' in db_type:
        precision, scale = map(int, db_type.split('Decimal(')[1].split(')')[0].split(','))
        return Type.Decimal(precision=precision, scale=scale)
    else:
        return Type.Default()


KNOWN_DB_TYPES = {
    'UInt8': Type.UInt8(),
    'UInt16': Type.UInt16(),
    'UInt32': Type.UInt32(),
    'UInt64': Type.UInt64(),
    'Int8': Type.Int8(),
    'Int16': Type.Int16(),
    'Int32': Type.Int32(),
    'Int64': Type.Int64(),
    'Float32': Type.Float32(),
    'Float64': Type.Float64(),
    'Decimal': (Type.Decimal, 'Parametric'),
    'String': Type.String(),
    'UUID': Type.UUID(),
    'Enum8': Type.Enum8(),
    'Enum16': Type.Enum16(),
    'Array(String)': Type.Array(items=Type.String()),
    'Array(UInt8)':  Type.Array(items=Type.UInt8()),
    'Array(UInt16)': Type.Array(items=Type.UInt16()),
    'Array(UInt32)': Type.Array(items=Type.UInt32()),
    'Array(UInt64)': Type.Array(items=Type.UInt64()),
    'Array(Int8)':   Type.Array(items=Type.Int8()),
    'Array(Int16)':  Type.Array(items=Type.Int16()),
    'Array(Int32)':  Type.Array(items=Type.Int32()),
    'Array(Int64)':  Type.Array(items=Type.Int64()),
    'Array(Float32)': Type.Array(items=Type.Float32()),
    'Array(Float64)': Type.Array(items=Type.Float64()),
    'Date': Type.Date(),
    'DateTime': Type.DateTime()
}


class ColumnsDef(object):
    @classmethod
    def column_names(cls, columns):
        if type(columns[0]) is str:
            return columns
        return zip(*columns)[0]

    @classmethod
    def column_type_factories(cls, columns):
        if type(columns[0]) is str: # columns is a list of field names
            type_factories = [Type.String()] * len(columns)
            return type_factories

        type_decls = zip(*columns)[1]
        type_factories = [None] * len(type_decls)

        for i, decl in enumerate(type_decls):
            if type(decl) is str:
                type_factories[i] = factory_from_db_type(decl)
            elif hasattr(decl, '__class__'):
                type_factories[i] = decl
            else:
                raise Exception("Unsupported columns definition! Expected Type ancestor or str with known type name, got: {v}".format(columns))

        return type_factories

    @classmethod
    def parse_into_typed_dict(cls, columns, *db_values):
        if type(columns[0]) is str:
            return dict(map(lambda (c, f, v): (c, f.from_db_value(v, column_name=c)), zip(columns, cls.column_type_factories(columns), db_values)))
        elif type(columns[0]) is tuple:
            return dict(map(lambda (c, f, v): (c[0], f.from_db_value(v, column_name=c)), zip(columns, cls.column_type_factories(columns), db_values)))
