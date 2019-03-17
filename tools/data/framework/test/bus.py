from redis import Redis
import unittest
import sys
import os


from ..bus import Connection
from ..base import *

from fixtures.redis_fixture import fixture_data

def import_fixture(redis_instance, data):
    redis_instance.flushdb()

    for k, v in data.items():
        redis_instance.set(k, v)



class FakeEntity1(DataObject): pass
class FakeEntity2(DataObject):
    @property
    @linked('campaign_id')
    def campaign(self):
        pass

    @property
    @linked('destination_id')
    def destination(self):
        pass


_ENTITIES = {
    'Offer': FakeEntity1,
    'Hits': FakeEntity2
}


class EmptyDatabusTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('TEST_REDIS_URL', None):
            raise Exception("\n\nFor safety reason, framework tests are running "
                            "only on test database instance.\nSet the"
                            "'TEST_REDIS_URL' environmental variable to proper Redis URL.\n")
        cls._redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
        cls._redis.flushdb()

    def setUp(self):
        self.redis = DatabusTestCase._redis
        self.bus = Connection(redis=self.redis, entities_meta=_ENTITIES)

    def test_shouldnt_fail_when_no_entities_saved_yet(self):
        l = list(self.bus.multiread('Offer'))
        self.assertEqual(len(l), 0)


class DatabusTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('TEST_REDIS_URL', None):
            raise Exception("\n\nFor safety reason, framework tests are running "
                            "only on test database instance.\nSet the"
                            "'TEST_REDIS_URL' environmental variable to proper Redis URL.\n")
        cls._redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
        import_fixture(cls._redis, fixture_data)

    @classmethod
    def tearDownClass(cls):
        cls._redis.flushdb()

    def setUp(self):
        self.redis = DatabusTestCase._redis
        self.bus = Connection(redis=self.redis, entities_meta=_ENTITIES)

    def test_sanity(self):
        bus = Connection(url=os.environ['TEST_REDIS_URL'], entities_meta=_ENTITIES)
        self.assertTrue(True)

    def test_multiread(self):
        some_random_index = 153
        for destination in self.bus.multiread('Offer'):
            self.assertEqual(destination._idx, 0, "First entry will have idx = 0")
            self.assertEqual(destination.url_template, 'https://jaunithuw.com/?h=9dad9c9097a736ce162988dc28d0dda60810115f&pci={external_id}&ppi={zone}')
            break

        self.assertEqual(len(list(self.bus.multiread('Offer'))), 12)

        for hit in self.bus.multiread('Hits', start=some_random_index):
            self.assertEqual(hit._idx, some_random_index)
            self.assertEqual(hit.click_id, '121427560658636800')
            self.assertEqual(hit.destination._idx, 7)
            self.assertEqual(hit.destination.url_template, 'https://jaunithuw.com/?h=f1b5821ac37e8e5104ede686ae9e3263edcfc6e6&pci={external_id}&ppi={zone}')
            break

        hits = list(self.bus.multiread('Hits', start=some_random_index, end=some_random_index + 100))
        self.assertEqual(len(hits), 101, "Should load all entries.")

    def test_readonly_pipe(self):
        offer1, offer2 = self.bus.readonly().by_id('Offer:[0]').by_id('Offer:[1]').execute()

        self.assertEqual(offer1._idx, 0)
        self.assertEqual(offer2._idx, 1)

        self.assertEqual(offer1.url_template, 'https://jaunithuw.com/?h=9dad9c9097a736ce162988dc28d0dda60810115f&pci={external_id}&ppi={zone}')
        self.assertEqual(offer2.url_template, 'https://jaunithuw.com/?h=0c6a4ddb2e8336632cf8de86770852dbb3a32560&pci={external_id}&ppi={zone}')
