from bus import *
from utils import *
from reporting import *

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

def fakebus(): return Fakebus()

def create_fake_entity(cls, entity_name, idx, **kwargs):
    return cls(bus=fakebus(), id="%s:[%s]" % (entity_name, idx), **kwargs)


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

def assert_reporting_object_instance(testcase, obj):
    testcase.assertIsNotNone(getattr(obj, 'into_db_columns'))
    testcase.assertIsNotNone(getattr(obj, 'into_db_row'))

    columns = obj.into_db_columns()
    # columns required by reporting
    testcase.assertIn('id', dict(columns).keys())
    testcase.assertEqual(dict(columns)['id'], ModelTypes.IDX)

    # columns required by Clickhouse
    testcase.assertIn('date_added', dict(columns).keys())
    testcase.assertEqual(dict(columns)['date_added'], ModelTypes.DATE)

    rows = obj.into_db_row()
    print rows
