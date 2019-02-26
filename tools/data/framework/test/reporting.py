import os
import unittest

from ipaddr import IPAddress, IPv4Address
from decimal import Decimal

from ..reporting import Database


class ReportingDbTestCase(unittest.TestCase):
    def setUp(self):
        if not os.environ.get('TEST_CLICKHOUSE_URL', None):
            raise Exception("""

For safety reason, framework tests are running only on test database instance.
Set the 'TEST_CLICKHOUSE_URL' environmental variable to proper Clickhouse URL.
            """)
            return # just in case

        self.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test')

    def tearDown(self):
        self.report_db.drop()

    def test_read_with_type_factories_trivial_one_result(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT 3 * 4 AS result;",
                                     columns=(('result', int),)):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(row['result'], 12)

    def test_read_with_type_factories_trivial_multi_result(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT 3 * 4 AS result, 2 + 2 as foo;",
                                     columns=(('result', int), ('foo', str))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(row['result'], 12)
            self.assertEqual(row['foo'], '4')

    def test_read_with_type_factories(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                     columns=(('user', str), ('address', IPAddress), ('elapsed', Decimal), ('memory_usage', int))):
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
                                     columns=(('user', str), ('address', IPAddress), ('elapsed', Decimal), ('memory_usage', int))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), IPv4Address)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)


    def test_read_with_db_type_names(self):
        db = self.report_db.connected()
        for row, i, total in db.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                     columns=(('user', 'String'), ('address', 'IPAddress'), ('elapsed', 'Decimal'), ('memory_usage', 'UInt64'))):
            self.assertEqual(total, 1)
            self.assertEqual(i, 0)
            self.assertEqual(type(row), dict)
            self.assertEqual(type(row['user']), str)
            self.assertEqual(type(row['address']), str)
            self.assertEqual(type(row['memory_usage']), int)
            self.assertEqual(type(row['elapsed']), Decimal)

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
        schema_rows = list(db.describe(db='system', table='processes'))
        self.assertEqual(len(schema_rows), 35)
        self.assertIn({'type': 'Int64', 'name': 'memory_usage'}, zip(*schema_rows)[0])
