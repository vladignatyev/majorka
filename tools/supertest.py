import requests
import unittest
from testtools import EnvironmentTestCase


class BigTest(EnvironmentTestCase):
    def setUp(self):
        super(BigTest, self).setUp()

    def test_simple_redirects(self):
        offers = [
            self.fixture.create_offer(name='simple test offer', url='http://test-url-1.com/?external_id={external_id}'),
            self.fixture.create_offer(name='simple test offer', url='http://test-url-2.com/?external_id={external_id}')
        ]
        self.fixture.create_campaign(name='test campaign', alias='alias', offer_ids=offers)

        url = self.majorka_url().join('alias') # http://domain.com/alias
        response = requests.get(url, allow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn('http://test-url', response.headers['location'])

        logged_hits = list(self.bus.multiread('Hits', start=0))

        self.assertEqual(len(logged_hits), 1)
        self.assertIn('python-requests', logged_hits[0].__dict__['dimensions']['useragent'])




if __name__ == '__main__':
    unittest.main()
