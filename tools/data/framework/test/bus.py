from redis import Redis
import unittest
import sys
import os


from ..bus import Connection

from redis_fixture import fixture_data

def import_fixture(redis_instance, data):
    redis_instance.flushdb()

    for k, v in data.items():
        redis_instance.set(k, v)


class DatabusTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('TEST_REDIS_URL', None):
            raise Exception("\n\nFor safety reason, framework tests are running "
                            "only on test database instance.\nSet the"
                            "'TEST_REDIS_URL' environmental variable to proper Redis URL.\n")

    def setUp(self):
        self.redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
        self.bus = Connection(redis=self.redis)

        import_fixture(self.redis, fixture_data)

    def tearDown(self):
        self.redis.flushdb()

    def test_sanity(self):
        bus = Connection(url=os.environ['TEST_REDIS_URL'])
        self.assertTrue(True)

    def test_multiread(self):
        some_random_index = 153
        for destination in self.bus.multiread('Offer'):
            self.assertEqual(destination._idx, 0, "First entry will have idx = 0")
            self.assertEqual(destination.url_template, 'http://google.com/?utm_source={zone}&subid={external_id}', "First entry should have proper field value.")
            break

        self.assertEqual(len(list(self.bus.multiread('Offer'))), 2, "Should load all entries.")

        for hit in self.bus.multiread('Hits', start=some_random_index):
            self.assertEqual(hit._idx, some_random_index, "Should have proper idx value")
            self.assertEqual(hit.click_id, '43', "Access to object field")
            self.assertEqual(hit.destination._idx, 0, "Access to linked object's _idx")
            self.assertEqual(hit.destination.url_template, 'http://google.com/?utm_source={zone}&subid={external_id}', "Access to linked object's field")
            break

        hits = list(self.bus.multiread('Hits', start=some_random_index, end=some_random_index + 100))
        self.assertEqual(len(hits), 101, "Should load all entries.")

    def test_readonly_pipe(self):
        offer1, offer2 = self.bus.readonly().by_id('Offer:[0]').by_id('Offer:[1]').execute()

        self.assertEqual(offer1._idx, 0)
        self.assertEqual(offer2._idx, 1)

        self.assertEqual(offer1.url_template, 'http://google.com/?utm_source={zone}&subid={external_id}')
        self.assertEqual(offer2.url_template, 'http://yandex.ru/?utm_source={zone}&subid={external_id}')
