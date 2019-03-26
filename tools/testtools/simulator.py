import json
from itertools import cycle
from requests import Request, Session
import time

from copy import deepcopy

from testtools import MajorkaFixture


class RequestFactory(object):
    def __init__(self, base_url):
        self.base_url = base_url

    def create_request_from_sample(self, sample_dict):
        url = self.get_url_for_sample(sample_dict)
        http_headers = self.get_headers_for_sample(sample_dict)
        return self.map_request_data_to_request_obj(method='GET', url=url, headers=http_headers)
        return Request('GET', url, headers=http_headers)

    def map_request_data_to_request_obj(self, url, headers, method='GET'):
        return Request(method, url, headers=headers)

    def get_headers_for_sample(self, sample_dict):
        # User agent
        http_headers = {
            'User-Agent': sample_dict.get('dimensions.useragent', 'trafficsampler/1.0')
        }

        # Google Chrome like headers
        http_headers['accept-encoding'] = 'gzip, deflate, br'
        http_headers['accept-language'] = 'en,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7,ru;q=0.6,zh-HK;q=0.5,zh-TW;q=0.4'
        http_headers['cache-control'] = 'max-age=0'
        http_headers['accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b2'

        return http_headers


    def get_url_for_sample(self, sample_dict):
        d = {
            'zone': sample_dict.get('dimensions.zone', ''),
            'connection_type': sample_dict.get('dimensions.connection_type', ''),
            'os': sample_dict.get('dimensions.os', ''),
            'creative_id': sample_dict.get('dimensions.creative_id', ''),
            'langcode': sample_dict.get('dimensions.langcode', ''),

            'cost': str(sample_dict.get('cost.value', '0.0')),
            'currency': sample_dict.get('cost.currency', 'usd')
        }

        return replace_placeholders_in_url(str(self.base_url), d)


def replace_placeholders_in_url(url, values):
    new_url = url
    for k, v in values.items():
        # todo: actually, the URL should contain placeholders in format
        #       {placeholder}, but `requests` escape curly braces `{` and `}`
        new_url = new_url.replace('%7B{key}%7D'.format(key=k), v)
    return new_url


class BasicTrafficSampler(object):
    def __init__(self, base_url, samples, state=-1, req_factory=None,
                 skip_keys=('time', 'campaign_id', 'destination_id', 'click_id', 'dimensions.external_id')):
        self._provided_samples = list(samples)
        self.skip_keys = skip_keys
        self.state = int(state)

        self.samples_pool = cycle(self._provided_samples)

        # RTFM: http://docs.python-requests.org/en/master/user/advanced/#request-and-response-objects
        self.session = Session()
        self.request_factory = req_factory or RequestFactory(base_url=base_url)

    def __iter__(self):
        return self

    def next(self, step=1):
        self.state += step
        new_sample = None
        for i in range(0, step):
            # new_sample = self.samples_pool.next()
            new_sample = next(self.samples_pool)

        clean_sample = self.clean_sample(new_sample)
        return self.request_factory.create_request_from_sample(clean_sample), clean_sample

    def send(self, request):
        prepared_request = self.session.prepare_request(request)
        settings = self.session.merge_environment_settings(prepared_request.url, {}, None, None, None)
        settings['allow_redirects'] = False
        return self.session.send(prepared_request, **settings)

    def forward(self, state):
        if self.state >= state:
            while self.state < state:
                self.next()
        else:
            raise ValueError("Cannot forward `back`. New state is < than current")

    def clean_sample(self, sample):
        # return dict(filter(lambda (k, v): k not in self.skip_keys, flat_items(sample)))
        return dict([(k,v) for k, v in flat_items(sample) if k not in self.skip_keys])

    def get_state():
        return self.state


# class Simulator(object):
#     ALIAS = 'simulated-alias'
#
#     def __init__(self, majorka_fixture, offers_models, traffic_sampler):
#         self.majorka = majorka_fixture
#         self.offers = offers_models
#         self.offer_ids = []
#         self.offers_count = len(offers_models)
#         self.sampler = traffic_sampler
#
#         self.hits_counter = 0
#
#     def setup(self):
#         self._build_campaign()
#         self._build_offers()
#
#     def run(self):
#         req = hit = self.sampler.next()
#         res = self.sampler.send(req)
#
#         loc = furl(res.headers['Location'])
#         offer_num = int(loc.host.split('http://test-url-')[1].split('.com',1)[0])
#         external_id = loc.args.get('external_id')
#
#         self.hits_counter += 1
#
#         offer_model = self.offers[offer_num]
#
#         if offer_model.convert_or_not(hit):
#             self.postback()
#
#     def postback(self):
#         pass
#         # requests.send('POST', # todo
#
#         # ....
#
#     def _build_campaign(self):
#         self.majorka.create_campaign(name='Simulated campaign',
#                                      alias=Simulator.ALIAS,
#                                      offer_ids=self.offer_ids)
#     def _build_offers(self):
#         for i in range(self.offers_count):
#             self.offer_ids += [
#                 self.majorka.create_offer(name='Simulated offer N{i}'.format(i=i),
#                                           url=('http://test-url-{i}'.format(i=i)) + '.com/?external_id={external_id}')
#             ]


def flat_items(d, key_separator='.'):
    """
    Flattens the dictionary containing other dictionaries like here: https://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys

    >>> example = {'a': 1, 'c': {'a': 2, 'b': {'x': 5, 'y' : 10}}, 'd': [1, 2, 3]}
    >>> flat = dict(flat_items(example, key_separator='_'))
    >>> assert flat['c_b_y'] == 10
    """
    for k, v in d.items():
        if type(v) is dict:
            for k1, v1 in flat_items(v, key_separator=key_separator):
                yield key_separator.join((k, k1)), v1
        else:
            yield k, v

def samples_from_fixture(fixture_data):
    """
    >>> fixture_data = {}
    >>> fixture_data['Hits:[10006]'] = '{"time":{"secs_since_epoch":1550532553,"nanos_since_epoch":380053396},"campaign_id":"Campaign:[0]","destination_id":"Offer:[2]","click_id":"121504936184778752","cost":{"value":85,"currency":"USD"},"dimensions":{"ua_type":"browser","os":"Android","zone":"847358","langcode":"el-GR","useragent":"Mozilla/5.0 (Linux; Android 7.0; Redmi Note 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","connection_type":"BROADBAND","ua_vendor":"Google","ua_name":"Chrome","creative_id":"","ip":"127.0.0.1","keywords":"","ua_category":"smartphone","ua_version":"72.0.3626.105","os_version":"7.0","language":"el-GR,el;q=0.9","external_id":"c0xg"}}'
    >>> fixture_data['Hits:[10002]'] = '{"time":{"secs_since_epoch":1550532542,"nanos_since_epoch":171201013},"campaign_id":"Campaign:[0]","destination_id":"Offer:[0]","click_id":"121504892744372224","cost":{"value":258,"currency":"USD"},"dimensions":{"zone":"813021","creative_id":"","ua_name":"Chrome","ua_vendor":"Google","keywords":"","connection_type":"MOBILE","useragent":"Mozilla/5.0 (Linux; Android 8.1.0; Redmi Note 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","external_id":"c0xv","ip":"127.0.0.1","langcode":"el-GR","ua_version":"72.0.3626.105","ua_category":"smartphone","os_version":"8.1.0","os":"Android","referer":"http://dolohen.com/afu.php?zoneid=1407888&var=813021","language":"el-GR,el;q=0.9","ua_type":"browser"}}'
    >>> fixture_data['Hits:_counter'] = '28343'
    >>> samples = samples_from_fixture(fixture_data)
    >>> assert len(samples) == 2
    >>> assert samples[0]['time']['secs_since_epoch'] == 1550532553
    >>> assert samples[1]['dimensions']['langcode'] == 'el-GR'
    """

    fixture_data_items_excluding_counter = zip(*filter(lambda (k, v): '_counter' not in k, fixture_data.items()))[1]
    # hits = map(json.loads, fixture_data_items_excluding_counter)
    hits = [json.loads(item) for item in fixture_data_items_excluding_counter]

    return hits

def with_cost(hits, value, currency='usd'):
    """
    Generator that mixes in fixed cost to every hit
    in hits fixture passed as `hits` argument

    >>> hits = [ \
                {'dimensions':{'useragent':'Chrome'}}, \
                {'dimensions':{'useragent':'Safari'}}, \
                {'dimensions':{'useragent':'Bot'}} \
                ]
    >>> assert len(hits) == 3
    >>> assert list(with_cost(hits, value='0.1', currency='eur')) == \
               [ \
                 { \
                 'cost': {'currency': 'eur', 'value': '0.1'}, \
                 'dimensions': {'useragent': 'Chrome'} \
                 }, \
                 { \
                 'cost': {'currency': 'eur', 'value': '0.1'}, \
                 'dimensions': {'useragent': 'Safari'} \
                 }, \
                 { \
                 'cost': {'currency': 'eur', 'value': '0.1'}, \
                 'dimensions': {'useragent': 'Bot'} \
                 } \
               ]
    """
    for hit in hits:
        hit['cost'] = {
            'value': value,
            'currency': currency
        }
        yield hit


def filter_dimension(hits, dimension, bad_values):
    """
    Filter out samples containing bad_values in dimension
    >>> hits = [{"time":{"secs_since_epoch":1550580200,"nanos_since_epoch":722891202},"campaign_id":"Campaign:[0]","destination_id":"Offer:[11]","click_id":"121367091113627648","cost":{"value":103,"currency":"USD"},"dimensions":{"external_id":"cuvq","useragent":"ApacheBench/2.3","ip":"127.0.0.1","zone":"AB_TEST","creative_id":"","connection_type":"MOBILE","keywords":""}}, \
                {"time":{"secs_since_epoch":1550580200,"nanos_since_epoch":718675710},"campaign_id":"Campaign:[0]","destination_id":"Offer:[1]","click_id":"121367091113627648","cost":{"value":103,"currency":"USD"},"dimensions":{"zone":"AB_TEST","useragent":"ApacheBench/2.3","keywords":"","creative_id":"","connection_type":"MOBILE","ip":"127.0.0.1","external_id":"cuv8"}}, \
                {"time":{"secs_since_epoch":1550580200,"nanos_since_epoch":716646242},"campaign_id":"Campaign:[0]","destination_id":"Offer:[2]","click_id":"121367091113627648","cost":{"value":103,"currency":"USD"},"dimensions":{"keywords":"","connection_type":"MOBILE","external_id":"cuvt","creative_id":"","zone":"AB_TEST","useragent":"ApacheBench/2.3","ip":"127.0.0.1"}} \
                ]
    >>> assert len(hits) == 3
    >>> filtered = filter_dimension(hits, 'connection_type', ('XDSL',))
    >>> assert len(filtered) == 3
    >>> filtered = filter_dimension(hits, 'connection_type', ('MOBILE',))
    >>> assert len(filtered) == 0
    """
    return [hit for hit in hits if not hit[u'dimensions'][dimension] in bad_values]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
