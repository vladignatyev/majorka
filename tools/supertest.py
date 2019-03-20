import subprocess
import requests
import unittest

import json

import testtools
from testtools import EnvironmentTestCase, hang, main
from testtools.simulator import BasicTrafficSampler, samples_from_fixture, with_cost

from testtools.hit_samples import fixture_data as hit_samples


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

    @hang
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


class RedirectsTest(EnvironmentTestCase):
    @hang
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


class TrafficTest(EnvironmentTestCase):
    @hang
    def test_simple(self):
        # todo: extract method majorka_url_for_campaign_by_alias
        base_url = self.majorka_url().join('alias').add({
            'connection_type': '{connection_type}',
            'zone': '{zone}',
            'cost': '{cost}',
            'currency': '{currency}'
        })

        filtered_samples = filter(lambda hit: hit[u'dimensions'][u'zone'] != '' and hit[u'dimensions'][u'zone'] != 'AB_TEST', samples_from_fixture(hit_samples))


        sampler = BasicTrafficSampler(base_url=base_url, samples=with_cost(filtered_samples, value='0.005'))

        offers = [
            self.fixture.create_offer(name='simple test offer', url='http://test-url-1.com/?external_id={external_id}'),
            self.fixture.create_offer(name='simple test offer', url='http://test-url-2.com/?external_id={external_id}')
        ]
        self.fixture.create_campaign(name='test campaign', alias='alias', offer_ids=offers)

        for i in range(1000):
            request = sampler.next()

            response = sampler.send(request)
            self.assertEqual(response.status_code, 302)
            self.assertIn('http://test-url', response.headers['location'])

        logged_hits = list(self.bus.multiread('Hits', start=0))

        self.assertEqual(len(logged_hits), 1000)
        self.assertEqual(logged_hits[0].__dict__['cost']['currency'], 'USD')
        self.assertEqual(logged_hits[0].__dict__['cost']['value'], 0.005 * 100000)

        self.assertNotIn('python-requests', logged_hits[0].__dict__['dimensions']['useragent'])
        self.assertIn('Mozilla/5.0', logged_hits[0].__dict__['dimensions']['useragent'])
        self.assertIn('Chrome/72.0.3626.105', logged_hits[0].__dict__['dimensions']['useragent'])


if __name__ == '__main__':
    testtools.main()
