import subprocess
import requests
import unittest

import json

import testtools
from testtools import EnvironmentTestCase, hang, main
from testtools.simulator import BasicTrafficSampler, samples_from_fixture, with_cost, filter_dimension, flat_items

from testtools.fixtures.hit_samples import fixture_data as hit_samples
from testtools.models import MultiDimensionDistribution

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

        result = subprocess.check_output(self._get_ab_args(url=self.majorka.majorka_url().join('alias')))

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

        url = self.majorka.majorka_url().join('alias') # http://domain.com/alias
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
        base_url = self.majorka.majorka_url().join('alias').add({
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
            request, _ = sampler.next()

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


class TrafficSamplerTest(EnvironmentTestCase):
    @hang
    def test_simple(self):
        offers = [
            self.fixture.create_offer(name='simple test offer', url='http://test-url-1.com/?external_id={external_id}'),
            self.fixture.create_offer(name='simple test offer', url='http://test-url-2.com/?external_id={external_id}')
        ]
        self.fixture.create_campaign(name='test campaign', alias='alias', offer_ids=offers)

        # create url template for campaign, that accepts connection type as a parameter
        base_url = self.majorka.majorka_url(campaign_alias='alias').add({
            'connection_type': '{connection_type}'
        })

        filtered_samples = filter_dimension(samples_from_fixture(hit_samples), 'zone', ('', 'AB_TEST',))

        sampler = BasicTrafficSampler(base_url=base_url, samples=with_cost(filtered_samples, value='0.005'))

        for i in range(100):
            request, _ = sampler.next()

            response = sampler.send(request)
            self.assertEqual(response.status_code, 302)
            self.assertIn('http://test-url', response.headers['location'])
            # print response.headers['location']


class ZoneSampleModelTest(EnvironmentTestCase):
    @hang
    def test_simple(self):
        # loading fixture data
        import random
        import json
        from furl import furl
        from testtools.fixtures.redis_fixture import fixture_data as rfix


        all_hits = [dict(flat_items(json.loads(rfix[k]))) for k in rfix.keys() if k.startswith('Hits') and '_counter' not in k]
        all_conversions = [dict(flat_items(json.loads(rfix[k]))) for k in rfix.keys() if k.startswith('Conversion') and '_counter' not in k]


        # build 'join table' by external_id
        external_id_to_hit = { item['dimensions.external_id']: item for item in all_hits }
        external_id_to_conversion = { item['external_id']: item for item in all_conversions }

        print len(list(all_hits))
        print len(list(all_conversions))

        # create offer models from hit fixture
        offer_ids = list(sorted(set([hit['destination_id'] for hit in all_hits])))
        offer_models = {offer_id: MultiDimensionDistribution()  for offer_id in offer_ids}

        print offer_ids

        # train offer models on saved traffic
        dimensions = [u'dimensions.zone', u'dimensions.connection_type', u'dimensions.langcode']
        for hit in all_hits:
            external_id = hit['dimensions.external_id']
            conv = external_id in external_id_to_conversion.keys()
            vec = [hit.get(d, None) for d in dimensions] + ['conv' if conv else 'noconv']
            offer_models[hit['destination_id']].eat(vec)

        # offer_models[offer_ids[1]].print_graph()

        self.fixture.create_campaign(name='test campaign', alias='alias', offer_ids=[self.fixture.create_offer(name='test offer {i}'.format(i=i), url='http://test-url-{i}.com/?external_id={external_id}'.format(i=i, external_id='{external_id}')) for i, offer_id in enumerate(offer_ids)])

        # create url template for campaign, that accepts connection type as a parameter
        base_url = self.majorka.majorka_url(campaign_alias='alias').add({
            'connection_type': '{connection_type}'
        })

        filtered_samples = filter_dimension(samples_from_fixture(hit_samples), 'zone', ('', 'AB_TEST',))

        sampler = BasicTrafficSampler(base_url=base_url, samples=with_cost(filtered_samples, value='0.005'))

        logged_conversions = list(self.bus.multiread('Conversions', start=0))
        self.assertEqual(len(logged_conversions), 0)

        conv = 0
        for i in range(1000):
            request, sample = sampler.next()
            response = sampler.send(request)
            self.assertEqual(response.status_code, 302)

            loc = furl(response.headers['Location'])
            # print loc
            offer_num = int(str(loc).split('http://test-url-', 1)[1].split('.com', 1)[0])
            external_id = loc.args['external_id']

            self.assertIn('http://test-url', str(loc))
            vec = [sample.get(d, None) for d in dimensions] + ['conv']
            # print vec
            prob = offer_models[offer_ids[offer_num]].probability(vec)
            # print prob

            if random.random() <= prob:
                # conversion occured
                postback_url = self.majorka.majorka_url().set(None, 'postback/').add({'external_id': external_id, 'revenue': '0.06', 'currency': 'usd', 'status': 'lead'})
                print postback_url
                requests.get(postback_url)
                conv += 1

        print conv

        logged_conversions = list(self.bus.multiread('Conversions', start=0))
        self.assertEqual(len(logged_conversions), conv)







if __name__ == '__main__':
    testtools.main()
