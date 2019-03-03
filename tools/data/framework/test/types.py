# -*- coding: utf-8 -*-
import unittest
from ..types import *


def _bool_str(value, true='==', false='!='):
    if value is True:
        return true
    elif value is False:
        return false
    else:
        raise ValueError("Incorrect value! Expected type `bool`, got `{type}`".format(type=type(value)))

class TypesTestCase(unittest.TestCase):
    def assertEqualByTypeAndValue(self, left, right, type_name):
        self.assertEqual(left, right,
                        "should produce same typed result and value "
                        "for type `{type}`:\n"
                        "\t types:  {type_left} {type_eq} {type_right}\n"
                        "\tvalues: {value_left} {value_eq} {value_right}\n".format(
                            type_eq=_bool_str(type(left) == type(right)),
                            type_left=type(left),
                            type_right=type(right),
                            value_left=left,
                            value_right=right,
                            value_eq=_bool_str(left == right),
                            type=type_name
                        ))

    def assertEqualType(self, left, right, msg):
        self.assertEqual(type(left), type(right), msg)

    def assertEqualDbType(self, left, right, type_name):
        self.assertEqual(left, right,
                        "into_db_type() and db_type_name should match "
                        "for type {type}. {left} != {right}".format(type=type_name, left=left, right=right))

    def test_known_simple_types(self):
        for db_type_name in filter(lambda k: not ('Decimal' in k or 'Date' in k), KNOWN_DB_TYPES.keys()):
            type_factory = KNOWN_DB_TYPES[db_type_name]
            safe_py_value = type_factory.default_py_value()

            self.assertEqualDbType(type_factory.into_db_type(), db_type_name, db_type_name)
            conv_result = type_factory.from_db_value(type_factory.into_db_value(py_value=safe_py_value))
            self.assertEqualByTypeAndValue(conv_result, safe_py_value, db_type_name)

    def test_array_types_integer(self):
        for item_type in filter(lambda k: 'Array' not in k and not ('Decimal' in k or 'Date' in k), KNOWN_DB_TYPES.keys()):
            some_array = [KNOWN_DB_TYPES[item_type].default_py_value()] * 3
            type_factory = Type.Array(items=KNOWN_DB_TYPES[item_type])
            self.assertEqual(type_factory.into_db_type(), 'Array({t})'.format(t=item_type))
            self.assertEqual(type_factory.from_db_value(type_factory.into_db_value(py_value=some_array)), some_array)

    def test_string_type(self):
        type_factory = Type.String()
        s = 'somestring'
        s2 = u'Строка'
        self.assertEqual(type_factory.into_db_type(), 'String')
        self.assertEqual(type_factory.into_db_value(py_value=s), "somestring")
        self.assertEqual(type_factory.into_db_value(py_value=s2), u"Строка")

    def test_decimal_type(self):
        type_factory = Type.Decimal32(5)
        self.assertEqual(type_factory.into_db_type(), 'Decimal32(5)')
        self.assertEqual(type_factory.into_db_value(py_value=Decimal('3.5555')), '3.5555')
        self.assertEqual(type_factory.from_db_value(type_factory.into_db_value(py_value=Decimal('3.5555'))), Decimal('3.5555'))
        type_factory = Type.Decimal64(5)
        self.assertEqual(type_factory.into_db_type(), 'Decimal64(5)')
        self.assertEqual(type_factory.into_db_value(py_value=Decimal('3.5555')), '3.5555')
        self.assertEqual(type_factory.from_db_value(type_factory.into_db_value(py_value=Decimal('3.5555'))), Decimal('3.5555'))
        type_factory = Type.Decimal128(5)
        self.assertEqual(type_factory.into_db_type(), 'Decimal128(5)')
        self.assertEqual(type_factory.into_db_value(py_value=Decimal('3.5555')), '3.5555')
        self.assertEqual(type_factory.from_db_value(type_factory.into_db_value(py_value=Decimal('3.5555'))), Decimal('3.5555'))

    def test_type_inferring(self):
        self.assertTrue(type(factory_from_db_type('UInt8')) is Type.UInt8)
        self.assertTrue(type(factory_from_db_type('String')) is Type.String)

        arr_str_type = factory_from_db_type('Array(String)')
        self.assertTrue(type(arr_str_type) is Type.Array)
        self.assertTrue(type(arr_str_type._items_type) is Type.String)

        arr_str_type = factory_from_db_type('Array(Int64)')
        self.assertTrue(type(arr_str_type) is Type.Array)
        self.assertTrue(type(arr_str_type._items_type) is Type.Int64)

        arr_str_type = factory_from_db_type('Array(Enum8)')
        self.assertTrue(type(arr_str_type) is Type.Array)
        self.assertTrue(type(arr_str_type._items_type) is Type.Enum8)

        arr_str_type = factory_from_db_type('Array(Float32)')
        self.assertTrue(type(arr_str_type) is Type.Array)
        self.assertTrue(type(arr_str_type._items_type) is Type.Float32)

        arr_str_type = factory_from_db_type('Array(UUID)')
        self.assertTrue(type(arr_str_type) is Type.Array)
        self.assertTrue(type(arr_str_type._items_type) is Type.UUID)

        arr_str_type = factory_from_db_type('StrangeUnknownType')
        self.assertTrue(type(arr_str_type) is Type.Default)

    def test_type_ipaddress(self):
        type_factory = Type.IPAddress()
        self.assertEqual(type_factory.into_db_type(), 'String')
        self.assertEqual(type_factory.into_db_value(py_value=IPAddress('192.168.9.40')), "192.168.9.40")
        self.assertEqual(type_factory.from_db_value(type_factory.into_db_value(py_value=IPAddress('192.168.9.40'))), IPAddress('192.168.9.40'))

    def test_type_array_of_strings_doesnt_eat_value(self):
        type_factory = Type.Array(items=Type.String())
        self.assertEqual(type_factory.into_db_value(py_value=['some', 'other']), "['some','other']")
