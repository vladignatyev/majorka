import subprocess
import requests
import unittest
import testtools
from testtools import EnvironmentTestCase


class AbTest(EnvironmentTestCase):
    def _get_ab_args(self, url):
        return [
            'ab',
            '-c',
            str(10),
            '-n',
            str(10000),
            str(url)
        ]

    def test_majorka_doesnt_miss_hits_under_load(self):
        logged_hits = list(self.bus.multiread('Hits', start=0))
        self.assertEqual(len(logged_hits), 0)

        offers = [
            self.fixture.create_offer(name='simple test offer', url='http://test-url-1.com/?external_id={external_id}'),
            self.fixture.create_offer(name='simple test offer', url='http://test-url-2.com/?external_id={external_id}')
        ]
        self.fixture.create_campaign(name='test campaign', alias='alias', offer_ids=offers)

        result = subprocess.check_output(self._get_ab_args(url=self.majorka_url().join('alias')))

        logged_hits = list(self.bus.multiread('Hits', start=0))
        self.assertEqual(len(logged_hits), 10000)


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

    def test_again(self):
        logged_hits = list(self.bus.multiread('Hits', start=0))
        self.assertEqual(len(logged_hits), 0)



if __name__ == '__main__':
    testtools.main()
