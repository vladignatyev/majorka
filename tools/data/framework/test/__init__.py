from bus import *
from utils import *
from reporting import *

from ..types import *


def fakebus(): return 'fakebus'

def create_fake_entity(cls, entity_name, idx, **kwargs):
    return cls(bus=fakebus(), id="%s:[%s]" % (entity_name, idx), **kwargs)


def assert_data_object_cls(testcase, cls):
    created = create_fake_entity(cls, 'Object', 0, testfield1='testvalue1', testfield2='testvalue2')

    # checking base object state
    testcase.assertEqual(created._id, 'Object:[0]')
    testcase.assertEqual(created._entity, 'Object')
    testcase.assertEqual(created._idx, 0)
    testcase.assertEqual(created._connection, 'fakebus')
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




    # def into_db_columns(self):
    #     obj_keys = sorted(filter(lambda k: k != 'id', self.__dict__.keys()))
    #     public_self_attrs = filter(lambda a: not a.startswith('_'), obj_keys)
    #     return [('id', ModelTypes.INTEGER)] + map(lambda k: (k, ModelTypes.STRING), public_self_attrs)
    #
    # def into_db_row(self, required_columns=None):
    #     if required_columns is None:
    #         db_columns = self.into_db_columns()
    #     else:
    #         db_columns = required_columns
    #
    #     db_row = [None] * len(db_columns)
    #     for i, (name, db_type) in enumerate(db_columns):
    #         raw_val = getattr(self, name)
    #         db_row[i] = (name, factory_into_db_type(db_type)(raw_val))
    #
    #     return dict(db_row)
