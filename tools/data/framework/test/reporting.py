# -*- coding: utf-8 -*-
import os
import unittest

from datetime import datetime, timedelta

from ipaddr import IPAddress, IPv4Address
from decimal import Decimal

from ..reporting import Database, SQLGenerator, DbError, ConnectionError
from ..tsv import TabSeparated, TabSeparatedError
from ..base import ReportingObject
from ..types import *
from asserts import create_fake_entity


class FakeEntity(ReportingObject):
    TABLE_NAME = 'fakeentity'
    @classmethod
    def into_db_columns(cls):
        return cls.default_columns() + \
        [('name', Type.String()),
         ('alias', Type.String()),

         ('offers', Type.Array(items=Type.Idx())),
         ('paused_offers', Type.Array(items=Type.Idx())),

         ('optimize', Type.Bool()),
         ('hit_limit_for_optimization', Type.Int64()),
         ('slicing_attrs', Type.Array(items=Type.String()))]



class ReportingDbTestCase(unittest.TestCase):
    # pragma: no cover
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('TEST_CLICKHOUSE_URL', None):
            raise Exception("\n\nFor safety reason, framework tests"
                            " are running only on test database instance.\n"
                            "Set the 'TEST_CLICKHOUSE_URL' environmental"
                            " variable to proper Clickhouse URL.\n")

    def setUp(self):
        self.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test', connection_timeout=1, data_read_timeout=1)
        self.report_db.connected()

    def tearDown(self):
        self.report_db.drop()

    def test_with_custom_sqlgen(self):
        gen = SQLGenerator('test2')
        db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test2', sqlgen=gen)
        self.assertEqual(db.sql, gen)

    def test_raises_connection_error(self):
        with self.assertRaises(ConnectionError):
            db = Database(url='http://example.com/nonexisting', db='nonexistingdb')

    def test_raises_db_error(self):
        db = self.report_db.connected()

        read_query = "invalid select * from system.processes;"
        write_query = "create table;"

        with self.assertRaises(DbError):
            db.read(sql=read_query, columns=())

        with self.assertRaises(DbError):
            db.write(sql=write_query)

    def test_read_simple_result(self):
        db = self.report_db.connected()
        self.assertTrue(db.read(sql="SELECT 2+2;", columns=(('result', Type.Int32()),), simple=True))

    def test_read_with_type_factories_trivial_one_result(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT 3 * 4 AS result;",
                                     columns=(('result', Type.Int32()),)):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(row['result'], 12)

    def test_read_with_type_factories_trivial_multi_result(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT 3 * 4 AS result, 2 + 2 as foo;",
                                     columns=(('result', Type.Int64()), ('foo', Type.String()))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(row['result'], 12)
            self.assertEqual(row['foo'], '4')

    def test_read_with_type_factories(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                     columns=(('user', Type.String()), ('address', Type.IPAddress()), ('elapsed', Type.Decimal64(5)), ('memory_usage', Type.Int64()))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), IPv4Address)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)

    def test_multiline_read_with_type_factories(self):
        db = self.report_db.connected()

        multiline_sql = """
        /* SQL comment */
        SELECT user,
               address,
               elapsed,
               memory_usage
        FROM
               system.processes;
        """
        for row, i, total in db.read(sql=multiline_sql,
                                     columns=(('user', Type.String()), ('address', Type.IPAddress()), ('elapsed', Type.Decimal64(5)), ('memory_usage', Type.Int64()))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), IPv4Address)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)

    def test_multiline_read_with_type_factories_win_endline_separator(self):
        db = self.report_db.connected()

        multiline_sql = "\r\n/* SQL comment */\r\nSELECT user,address,\r\nelapsed,\r\nmemory_usage\r\nFROM\r\nsystem.processes;\r\n"
        for row, i, total in db.read(sql=multiline_sql,
                                     columns=(('user', Type.String()), ('address', Type.IPAddress()), ('elapsed', Type.Decimal64(5)), ('memory_usage', Type.Int64()))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), IPv4Address)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)

    def test_multiline_write(self):
        db = self.report_db.connected()

        multiline_sql = """
        /* SQL comment: we create some fake table */
        CREATE TABLE IF NOT EXISTS test.fakeentity
        (
            id UInt64,
            date_added Date DEFAULT today(),
            name String,
            alias String,
            offers Array(UInt64),
            paused_offers Array(UInt64),
            optimize UInt8,
            hit_limit_for_optimization Int64,
            slicing_attrs Array(String)
        ) ENGINE = MergeTree(date_added, (id, date_added), 8192)
        """
        self.assertTrue(db.write(sql=multiline_sql))

    def test_read_with_db_type_names(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                     columns=(('user', 'String'), ('address', 'IPAddress'), ('elapsed', 'Decimal64(5)'), ('memory_usage', 'UInt64'))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), str)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)

    def test_read_with_db_type_names_simple_columns_declaration(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                     columns=('user', 'address', 'elapsed', 'memory_usage')):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), str)
            self.assertEqual(type(row['memory_usage']), str)
            self.assertEqual(type(row['elapsed']), str)

    def test_read_list(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;"):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), list)
            self.assertEqual(len(row), 4)
            self.assertTrue(all([type(item) == str for item in row]))

    def test_describe(self):
        db = self.report_db.connected()
        columns = db.describe(db='system', table='processes')
        d = dict(columns)
        self.assertEqual(len(columns), 35)
        self.assertTrue(type(d['memory_usage']) is Type.Int64)

    def test_describe_query(self):
        db = self.report_db.connected()
        columns = db.describe_query("SELECT 2+2 AS result;")
        d = dict(columns)
        self.assertEqual(len(columns), 1)
        self.assertTrue(type(d['result']) is Type.UInt16)

    def test_use_describe_for_typed_reading_from_table_with_unknown_schema(self):
        db = self.report_db.connected()

        query = "select * from system.processes;"
        schema = db.describe(db='system', table='processes')

        for row, i, l in db.read(sql=query, columns=schema):
            self.assertEqual(l, 1)
            self.assertEqual(type(row['port']), int)
            self.assertEqual(type(row['Settings.Values']), list)
            self.assertEqual(type(row['ProfileEvents.Values']), list)
            self.assertTrue(all(map(lambda item: type(item) == int, row['ProfileEvents.Values'])))
            self.assertTrue(type(row['http_method']) == int)
            self.assertTrue(row['http_method'])

    def test_response_type_checking_issue_10(self):
        db = self.report_db.connected()

        column_names = ['user', 'address', 'elapsed', 'memory_usage']
        columns = filter(lambda (column_name, column_type): column_name in column_names, db.describe('processes', db='system'))
        with self.assertRaises(KeyError):
            # result of request is a tuple (user, address, elapsed, memory_usage)
            # so when we try to access by key from column_names, we got KeyError
            for row, i, total in (db.read(sql="SELECT (user, address, elapsed, memory_usage) FROM system.processes", columns=columns)):
                list(map(lambda k: row[k], column_names))

        # but if we investigate a query first, using describe_query
        better_columns = db.describe_query(sql="SELECT (user, address, elapsed, memory_usage) FROM system.processes")
        for row, i, total in (db.read(sql="SELECT (user, address, elapsed, memory_usage) FROM system.processes", columns=better_columns)):
            list(row)

        pass

    def test_data_create_and_insert(self):
        now = datetime.now()

        rows = [
            ['foo', now, 123, [1,2,3]],
            ['bar', now, 666, [6,6,6]],
            ['baz', now, 321, [3,2,1]]
        ]

        columns = (('name', Type.String()),
                   ('date_added', Type.Date()),
                   ('value', Type.Int32()),
                   ('set', Type.Array(items=Type.Int32())))

        db = self.report_db.connected()

        self.maxDiff = None
        self.assertTrue(db.write(db.sql.create_table(table='testtable',
                                                     date_column='date_added',
                                                     index=('name',),
                                                     columns=columns)))

        self.assertTrue(db.write(db.sql.insert_values(table='testtable',
                                                      values=rows,
                                                      columns=columns)))

        from_db = list(db.read(sql="SELECT * FROM test.testtable", columns=columns))
        self.assertEqual(len(from_db), 3)

        rows_as_dicts_from_db = map(lambda (row, i, total): row, from_db)

        date_added = datetime(now.year, now.month, now.day)
        for row in rows_as_dicts_from_db:
            if row['name'] == 'foo':
                self.assertEqual(row['value'], 123)
                self.assertEqual(row['set'], [1,2,3])
                self.assertEqual(row['date_added'], date_added)
            elif row['name'] == 'bar':
                self.assertEqual(row['value'], 666)
                self.assertEqual(row['set'], [6,6,6])
                self.assertEqual(row['date_added'], date_added)
            elif row['name'] == 'baz':
                self.assertEqual(row['value'], 321)
                self.assertEqual(row['set'], [3,2,1])
                self.assertEqual(row['date_added'], date_added)
            else:
                self.fail("unexpected row result")

    def test_data_create_and_insert_with_improper_dimensions(self):
        now = datetime.now()

        rows = [
            ['foo', now, 123, [1,2,3]],
            ['bar', now, 666],
            ['baz', now]
        ]

        columns = (('name', Type.String()),
                   ('date_added', Type.Date()),
                   ('value', Type.Int32()),
                   ('set', Type.Array(items=Type.Int32())))

        columns_invalid = (('name', Type.String()),
                           ('date_added', Type.Date()))

        db = self.report_db.connected()

        self.maxDiff = None
        self.assertTrue(db.write(db.sql.create_table(table='testtable_improper_dimensions',
                                                     date_column='date_added',
                                                     index=('name',),
                                                     columns=columns)))

        with self.assertRaises(TabSeparatedError) as context:
            db.write(db.sql.insert_values(table='testtable',
                                          values=rows,
                                          columns=columns))
        self.assertIn('dimensions for every row should match dimension', context.exception.message)

        with self.assertRaises(Exception) as context:
            db.write(db.sql.insert_values(table='testtable',
                                          values=rows,
                                          columns=columns_invalid))

        self.assertIn('Dimensions of `values` and `columns` definition should match', context.exception.message)

        self.assertEqual(len(list(db.read(sql='SELECT * FROM test.testtable_improper_dimensions'))), 0, "should not alter data in table")


    def test_data_create_and_insert_with_simple_column_declaration(self):
        now = datetime.now()

        rows = [
            ['foo', '2018-02-28', 123, '[1,2,3]'],
            ['bar', '2018-02-28', 666, '[6,6,6]'],
            ['baz', '2018-02-28', 321, '[3,2,1]']
        ]

        columns = ('name', 'date_added', 'value', 'set')

        columns_spec_for_table_create = (('name', Type.String()),
                                           ('date_added', Type.Date()),
                                           ('value', Type.Int32()),
                                           ('set', Type.Array(items=Type.Int32())))

        db = self.report_db.connected()

        self.maxDiff = None
        self.assertTrue(db.write(db.sql.create_table(table='testtable_insert_with_simple_column_declaration',
                                                     date_column='date_added',
                                                     index=('name',),
                                                     columns=columns_spec_for_table_create)))

        self.assertTrue(db.write(db.sql.insert_values(table='testtable_insert_with_simple_column_declaration',
                                                      values=rows,
                                                      columns=columns)))

        self.assertEqual(len(list(db.read(sql='SELECT * FROM test.testtable_insert_with_simple_column_declaration'))), 3, "should not alter data in table")



class TabSeparatedTestCase(unittest.TestCase):
    def test_trivial(self):
        ts = TabSeparated(data=())
        self.assertEqual(ts.generate(), "")

    def test_normal(self):
        ts = TabSeparated(data=(('Petya', 22), ('Masha', 19), (u'Василий', 34)))
        self.assertMultiLineEqual(ts.generate(), u"Petya\t22\nMasha\t19\nВасилий\t34", "should normally generate tab separated output")

    def test_with_tabs(self):
        ts = TabSeparated(data=(('Petya', 22), ('H\tacker\t', 19), (u'Василий', 34)))
        with self.assertRaises(TabSeparatedError) as context:
            ts.generate()

        self.assertIn("TAB char is disallowed", context.exception.message)

        ts_dims = TabSeparated(data=(('Petya', 22), ('Hacker',), (u'Василий', 34)))
        with self.assertRaises(TabSeparatedError) as context:
            ts_dims.generate()

        self.assertIn("dimensions for every row should match", context.exception.message)



class SQLGeneratorTestCase(unittest.TestCase):
    def test_hello(self):
        gen = SQLGenerator(db_name='test')
        self.assertEqual(gen.hello(), "SELECT 1;")

    def test_describe(self):
        gen = SQLGenerator(db_name='test')
        self.assertEqual(gen.describe('sometable', from_db='test'), "DESCRIBE TABLE test.sometable;")
        self.assertEqual(gen.describe('sometable'), "DESCRIBE TABLE test.sometable;")
        self.assertEqual(gen.describe('processes', from_db='system'), "DESCRIBE TABLE system.processes;")

    def test_describe_query(self):
        gen = SQLGenerator(db_name='test')
        self.assertEqual(gen.describe_query("SELECT 2*2 as result"), "DESCRIBE (SELECT 2*2 as result);")
        self.assertEqual(gen.describe_query("SELECT 2*2 as result;"), "DESCRIBE (SELECT 2*2 as result);")

    def test_create_database(self):
        gen = SQLGenerator(db_name='test')
        self.assertEqual(gen.create_database(), "CREATE DATABASE IF NOT EXISTS \"test\";")
        gen = SQLGenerator(db_name='another')
        self.assertEqual(gen.create_database(), "CREATE DATABASE IF NOT EXISTS \"another\";")

    # def test_db_error_decimal_data_type_must_have_precision_and_scale(self):
    #     self.fail('Not implemented yet.')
    #
    # def test_insert_query_should_prepare_dicts_at_his_own_not_expecting_the_plain_list_of_values_if_columns_have_necessary_declarations(self):
    #     self.fail('Not implemented yet.')
    #
    # def test_insert_query_should_support_providing_custom_type_factories_in_column_declaration(self):
    #     self.fail('Not implemented yet.')

    def test_insert_values_extended_col_def(self):
        gen = SQLGenerator(db_name='test')

        rows = [
            ['foo', 123, [1,2,3]],
            ['bar', 666, [6,6,6]],
            ['baz', 321, [3,2,1]]
        ]

        columns = (('name', Type.String()),
                   ('value', Type.Int64()),
                   ('set', Type.Array(items=Type.Int32())))

        sql = gen.insert_values(table='sometable',
                                    values=rows,
                                    columns=columns)
        expected = """INSERT INTO test.sometable (name, value, set) FORMAT TabSeparated
foo\t123\t[1,2,3]
bar\t666\t[6,6,6]
baz\t321\t[3,2,1]"""
        self.assertMultiLineEqual(sql, expected)

    def test_create_table_for_entity(self):
        gen = SQLGenerator(db_name='test')

        fake_entity = create_fake_entity(FakeEntity,
                                       entity_name='Fake',
                                       idx=12,

                                       name='Some fake name',
                                       alias='fake alias',
                                       paused_offers=[1,2,3],
                                       offers=[0,1,2,3],

                                       optimize=False,
                                       hit_limit_for_optimization=50,
                                       slicing_attrs=['someattr'])

        self.maxDiff = None
        self.assertMultiLineEqual(gen.create_table_for_reporting_object(fake_entity), \
        """
        CREATE TABLE IF NOT EXISTS test.fakeentity
        (
            id UInt64,
            date_added Date,
            name String,
            alias String,
            offers Array(UInt64),
            paused_offers Array(UInt64),
            optimize UInt8,
            hit_limit_for_optimization Int64,
            slicing_attrs Array(String)
        ) ENGINE = MergeTree(date_added, (id, date_added), 8192)""")
