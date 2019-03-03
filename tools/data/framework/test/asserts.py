# pragma: no cover
from ..types import *


class Fakebus(object):
    class FakePipe(object):
        def __init__(self):
            self.objs = []

        def execute(self):
            return self.objs

        def by_id(linked_obj_id):
            if self.objs is None:
                self.objs = []
            self.objs += [linked_obj_id]

    def readonly(self):
        return self.FakePipe()

def create_fake_entity(cls, entity_name, idx, **kwargs):
    return cls(bus=Fakebus(), id="%s:[%s]" % (entity_name, idx), **kwargs)

def assert_data_object_cls(testcase, cls):
    created = create_fake_entity(cls, 'Object', 0, testfield1='testvalue1', testfield2='testvalue2')

    # checking base object state
    testcase.assertEqual(created._id, 'Object:[0]')
    testcase.assertEqual(created._entity, 'Object')
    testcase.assertEqual(created._idx, 0)
    testcase.assertEqual(type(created._connection), Fakebus)
    testcase.assertEqual(created.testfield1, 'testvalue1')
    testcase.assertEqual(created.testfield2, 'testvalue2')

def assert_reporting_object_cls(testcase, cls):
    testcase.assertIsNotNone(getattr(cls, 'into_db_columns'))
    testcase.assertIsNotNone(getattr(cls, 'into_db_row'))
    testcase.assertIsNotNone(getattr(cls, 'INDEX'))
    testcase.assertIsNotNone(getattr(cls, 'TABLE_NAME'))
    testcase.assertEqual(type(getattr(cls, 'INDEX')), tuple)
    testcase.assertIn('id', getattr(cls, 'INDEX'))
    testcase.assertIn('date_added', getattr(cls, 'INDEX'))

def assert_reporting_object_instance(testcase, obj):
    testcase.assertIsNotNone(getattr(obj, 'into_db_columns'))
    testcase.assertIsNotNone(getattr(obj, 'into_db_row'))

    columns = obj.into_db_columns()
    # columns required by Clickhouse
    testcase.assertIn('id', dict(columns).keys())
    testcase.assertEqual(type(dict(columns)['id']), type(Type.Int64()))
    testcase.assertIn('date_added', dict(columns).keys())
    testcase.assertEqual(type(dict(columns)['date_added']), type(Type.Date()))

    rows = obj.into_db_row()
