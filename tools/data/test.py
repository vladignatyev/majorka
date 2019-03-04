import unittest
import os

from decimal import Decimal
from redis import Redis

from framework.bus import Connection as BusConnection
from framework.test.asserts import assert_data_object_cls, assert_reporting_object_cls, assert_reporting_object_instance, create_fake_entity

from model import *

from test.fixtures.first_import import fixture_data as first_import_fixture
from test.fixtures.first_import_hits import fixture_data as first_import_hits_fixture
from test.fixtures.add_hits import fixture_data as add_hits_fixture
from test.fixtures.add_hits_with_automigration import fixture_data as add_hits_with_automigration_fixture
from test.fixtures.add_hits_with_missed_fields import fixture_data as add_hits_with_missed_fields_fixture
from test.fixtures.add_conversions import fixture_data as add_conversions

def create_fake_campaign(idx=0):
    return create_fake_entity(Campaign,
                               entity_name='Campaign',
                               idx=idx,

                               name='Test campaign',
                               alias='testalias',
                               paused_offers=[],
                               offers=[],

                               optimize=False,
                               optimization_paused=True,
                               hit_limit_for_optimization=50,
                               slicing_attrs=[])

def create_fake_hit(idx=0):
    return create_fake_entity(Hit,
                              entity_name='Hits',
                              idx=idx,
                              dimensions={'test_dimension': 'dimension_value'},
                              campaign_id='Campaign:[0]',
                              destination_id='Offer:[0]',
                              cost={'value':Decimal(0.001), 'currency': 'USD'},
                              time={'secs_since_epoch': 10000000}
                              )

def create_fake_conversion(idx=0):
    return create_fake_entity(Conversion,
                             entity_name='Conversions',
                             idx=idx,
                             external_id='abc',
                             revenue={'value':Decimal(0.05), 'currency': 'USD'},
                             status='lead',
                             time={'secs_since_epoch': 10000000}
                             )

class ModelTestcase(unittest.TestCase):
    def test_models_as_data_objects(self):
        assert_data_object_cls(self, Campaign)
        assert_data_object_cls(self, Offer)
        assert_data_object_cls(self, Conversion)
        assert_data_object_cls(self, Hit)

    def test_models_as_reporting_object_cls(self):
        assert_reporting_object_cls(self, Campaign)
        assert_reporting_object_cls(self, Offer)
        assert_reporting_object_cls(self, Conversion)
        assert_reporting_object_cls(self, Hit)

    def test_models_as_reporting_object(self):
        fake_campaign = create_fake_campaign()
        fake_offer = create_fake_entity(Offer, entity_name='Offer', idx=0)
        fake_conversion = create_fake_conversion()
        fake_hit = create_fake_hit()

        assert_reporting_object_instance(self, fake_campaign)
        assert_reporting_object_instance(self, fake_offer)
        assert_reporting_object_instance(self, fake_conversion)
        assert_reporting_object_instance(self, fake_hit)


class ImportingTestcase(unittest.TestCase):
    @classmethod
    def import_redis_fixture(cls, data):
        for k, v in data.items():
            cls.redis.set(k, v)

    @classmethod
    def setUpClass(cls):
        if not os.environ.get('TEST_REDIS_URL', None):
            raise Exception("\n\nFor safety reason, framework tests are running "
                            "only on test database instance.\nSet the"
                            "'TEST_REDIS_URL' environmental variable to proper Redis URL.\n")

        if not os.environ.get('TEST_CLICKHOUSE_URL', None):
            raise Exception("\n\nFor safety reason, framework tests"
                            " are running only on test database instance.\n"
                            "Set the 'TEST_CLICKHOUSE_URL' environmental"
                            " variable to proper Clickhouse URL.\n")

        cls.redis = Redis.from_url(os.environ['TEST_REDIS_URL'])
        cls.bus = BusConnection(redis=cls.redis, entities_meta=ENTITIES)
        cls.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test', connection_timeout=1, data_read_timeout=1)

    def setUp(self):
        self.redis.flushdb()
        self.report_db.connected().drop()

    def assertEntitiesTablesDoNotExist(self):
        with self.assertRaises(DbError):
            self.report_db.connected().describe(table='campaigns')
        with self.assertRaises(DbError):
            self.report_db.connected().describe(table='offers')
        with self.assertRaises(DbError):
            self.report_db.connected().describe(table='hits')
        with self.assertRaises(DbError):
            self.report_db.connected().describe(table='conversions')

    def assertEntityTablesInitialized(self):
        self.report_db.connected().describe(table='campaigns')
        self.report_db.connected().describe(table='offers')
        self.report_db.connected().describe(table='conversions')
        self.report_db.connected().describe(table='hits')

    def assertNoEntitiesSavedYet(self):
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Campaign', ENTITIES['Campaign']), (0,0))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Offer', ENTITIES['Offer']), (0,0))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Conversions', ENTITIES['Conversions']), (0,0))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Hits', ENTITIES['Hits']),(0,0))

    def test_load_entity_loads_objects_and_creates_tables(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)
        self.import_redis_fixture(first_import_fixture)
        self.assertEntitiesTablesDoNotExist()

        self.assertEntitiesTablesDoNotExist()

        campaigns_to_save = data_import.load_entity('Campaign', ENTITIES['Campaign'])
        offers_to_save = data_import.load_entity('Offer', ENTITIES['Offer'])
        conversions_to_save = data_import.load_entity('Conversions', ENTITIES['Conversions'])
        hits_to_save = data_import.load_entity('Hits', ENTITIES['Hits'])

        self.assertEntityTablesInitialized()
        self.assertNoEntitiesSavedYet()

        # check that loaded entity objects count is equal to declared in fixture
        self.assertEqual(len(campaigns_to_save), 1)
        self.assertEqual(len(offers_to_save), 3)
        self.assertEqual(len(conversions_to_save), 2)
        self.assertEqual(len(hits_to_save), 1)

    def test_first_import_simple_entities(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)
        self.import_redis_fixture(first_import_fixture)

        data_import.load_simple_entities()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Campaign', ENTITIES['Campaign']), (0, 1))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Offer', ENTITIES['Offer']), (2, 3))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Conversions', ENTITIES['Conversions']), (1, 2))

        stored_campaigns = zip(*list(self.report_db.connected().read(sql="select * from test.campaigns;",
                                                                columns=self.report_db.connected().describe(table='campaigns'))))[0]

        self.assertEqual(len(stored_campaigns), 1)
        self.assertEqual(stored_campaigns[0]['name'], "testcampaign")
        self.assertEqual(stored_campaigns[0]['alias'], "majorka")
        self.assertEqual(stored_campaigns[0]['offers'], [0,1,2])
        self.assertEqual(stored_campaigns[0]['paused_offers'], [])
        self.assertEqual(stored_campaigns[0]['optimize'], True)
        self.assertEqual(stored_campaigns[0]['slicing_attrs'], ['zone', 'connection_type'])

        stored_offers = zip(*list(self.report_db.connected().read(sql="select * from test.offers;",
                                                                columns=self.report_db.connected().describe(table='offers'))))[0]

        self.assertEqual(len(stored_offers), 3)
        self.assertEqual(stored_offers[0]['name'], "Video Player - Big Play")
        self.assertEqual(stored_offers[0]['url_template'], "https://jaunithuw.com/?h=9dad9c9097a736ce162988dc28d0dda60810115f&pci={external_id}&ppi={zone}")
        self.assertEqual(stored_offers[1]['name'], "18+ Tap if 18")
        self.assertEqual(stored_offers[1]['url_template'], "https://jaunithuw.com/?h=13451ad5d7bd5e0551226bbd1eaf962d8ca12d3d&pci={external_id}&ppi={zone}")
        self.assertEqual(stored_offers[2]['name'], "Video Player - Video blocked")
        self.assertEqual(stored_offers[2]['url_template'], "https://jaunithuw.com/?h=befc5c3695c9aaa75255f9b467f2c4a4889c5332&pci={external_id}&ppi={zone}")

        stored_conversions = zip(*list(self.report_db.connected().read(sql="select * from test.conversions;",
                                                                columns=self.report_db.connected().describe(table='conversions'))))[0]

        self.assertEqual(len(stored_conversions), 2)
# fixture_data['Conversions:[0]'] = '{"time":{"secs_since_epoch":1550513804,"nanos_since_epoch":380712442},"external_id":"c3vx","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[0]['external_id'], "c3vx")
        self.assertEqual(stored_conversions[0]['status'], "lead")
        self.assertEqual(stored_conversions[0]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[0]['time'], datetime.utcfromtimestamp(1550513804))
# fixture_data['Conversions:[1]'] = '{"time":{"secs_since_epoch":1550522181,"nanos_since_epoch":838230915},"external_id":"crub","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[1]['external_id'], "crub")
        self.assertEqual(stored_conversions[1]['status'], "lead")
        self.assertEqual(stored_conversions[1]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[1]['time'], datetime.utcfromtimestamp(1550522181))

    def test_import_simple_entities_incrementally(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)

        self.import_redis_fixture(first_import_fixture)
        data_import.load_simple_entities()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Campaign', ENTITIES['Campaign']), (0, 1))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Offer', ENTITIES['Offer']), (2, 3))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Conversions', ENTITIES['Conversions']), (1, 2))

        self.import_redis_fixture(add_conversions)
        data_import.load_simple_entities()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Campaign', ENTITIES['Campaign']), (0, 1))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Offer', ENTITIES['Offer']), (2, 3))
        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Conversions', ENTITIES['Conversions']), (3, 4))

        stored_conversions = zip(*list(self.report_db.connected().read(sql="select * from test.conversions;",
                                                                columns=self.report_db.connected().describe(table='conversions'))))[0]

        self.assertEqual(len(stored_conversions), 4)
# fixture_data['Conversions:[0]'] = '{"time":{"secs_since_epoch":1550513804,"nanos_since_epoch":380712442},"external_id":"c3vx","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[0]['external_id'], "c3vx")
        self.assertEqual(stored_conversions[0]['status'], "lead")
        self.assertEqual(stored_conversions[0]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[0]['time'], datetime.utcfromtimestamp(1550513804))
# fixture_data['Conversions:[1]'] = '{"time":{"secs_since_epoch":1550522181,"nanos_since_epoch":838230915},"external_id":"crub","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[1]['external_id'], "crub")
        self.assertEqual(stored_conversions[1]['status'], "lead")
        self.assertEqual(stored_conversions[1]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[1]['time'], datetime.utcfromtimestamp(1550522181))
# fixture_data['Conversions:[2]'] = '{"time":{"secs_since_epoch":1550533804,"nanos_since_epoch":380712442},"external_id":"c08m","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[2]['external_id'], "c08m")
        self.assertEqual(stored_conversions[2]['status'], "lead")
        self.assertEqual(stored_conversions[2]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[2]['time'], datetime.utcfromtimestamp(1550533804))
# fixture_data['Conversions:[3]'] = '{"time":{"secs_since_epoch":1550552181,"nanos_since_epoch":838230915},"external_id":"c08a","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
        self.assertEqual(stored_conversions[3]['external_id'], "c08a")
        self.assertEqual(stored_conversions[3]['status'], "lead")
        self.assertEqual(stored_conversions[3]['revenue'], Decimal('0.06'))
        self.assertEqual(stored_conversions[3]['time'], datetime.utcfromtimestamp(1550552181))

    def test_first_import_dynamic_entity_hits(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)

        self.import_redis_fixture(first_import_hits_fixture)
        data_import.load_hits()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Hits', ENTITIES['Hits']), (8, 9))

        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        self.assertEqual(len(stored_hits), 9)
        data_import.load_hits()
        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        # should not duplicate any objects
        self.assertEqual(len(stored_hits), 9)

# fixture_data['Hits:[0]'] = '{"time":{"secs_since_epoch":1550531809,"nanos_since_epoch":999425442},"campaign_id":"Campaign:[0]","destination_id":"Offer:[0]","click_id":"121501819418456064","cost":{"value":86,"currency":"USD"},"dimensions":{"referer":"http://constintptr.com/afu.php?zoneid=1407888&var=1675303","language":"el-GR,el;q=0.9","langcode":"el-GR","useragent":"Mozilla/5.0 (Linux; Android 8.0.0; LDN-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","zone":"1675303","connection_type":"BROADBAND","os_version":"8.0.0","ua_vendor":"Google","os":"Android","creative_id":"","ua_category":"smartphone","ua_version":"72.0.3626.105","ua_type":"browser","ua_name":"Chrome","keywords":"","ip":"127.0.0.1","external_id":"c08x"}}'
        self.assertEqual(stored_hits[0]['time'], datetime.utcfromtimestamp(1550531809))
        self.assertEqual(stored_hits[0]['click_id'], '121501819418456064')
        self.assertEqual(stored_hits[0]['cost'], Decimal('0.00086'))
        self.assertEqual(stored_hits[0]['dim_referer'], "http://constintptr.com/afu.php?zoneid=1407888&var=1675303")
        self.assertEqual(stored_hits[0]['dim_language'], "el-GR,el;q=0.9")
        self.assertEqual(stored_hits[0]['dim_os'], "Android")
        self.assertEqual(stored_hits[0]['dim_zone'], "1675303")
        self.assertEqual(stored_hits[0]['dim_external_id'], "c08x")
        self.assertEqual(stored_hits[0]['campaign'], 0)
        self.assertEqual(stored_hits[0]['destination'], 0)
        self.assertEqual(list(sorted(filter(lambda k: 'dim_' in k, stored_hits[0].keys()))), ['dim_connection_type', 'dim_creative_id', 'dim_external_id', 'dim_ip', 'dim_keywords', 'dim_langcode', 'dim_language', 'dim_os', 'dim_os_version', 'dim_referer', 'dim_ua_category', 'dim_ua_name', 'dim_ua_type', 'dim_ua_vendor', 'dim_ua_version', 'dim_useragent', 'dim_zone'])

# fixture_data['Hits:[8]'] = '{"time":{"secs_since_epoch":1550531842,"nanos_since_epoch":638799152},"campaign_id":"Campaign:[0]","destination_id":"Offer:[11]","click_id":"121501958946164736","cost":{"value":85,"currency":"USD"},"dimensions":{"ua_name":"Chrome","creative_id":"","keywords":"","external_id":"crub","langcode":"el-GR","ua_category":"smartphone","os":"Android","os_version":"7.0","ua_vendor":"Google","ip":"127.0.0.1","ua_type":"browser","ua_version":"72.0.3626.105","useragent":"Mozilla/5.0 (Linux; Android 7.0; Redmi Note 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","connection_type":"BROADBAND","language":"el-GR,el;q=0.9,en;q=0.8","zone":"756262"}}'
        self.assertEqual(stored_hits[8]['time'], datetime.utcfromtimestamp(1550531842))
        self.assertEqual(stored_hits[8]['click_id'], '121501958946164736')
        self.assertEqual(stored_hits[8]['cost'], Decimal('0.00085'))
        self.assertEqual(stored_hits[8]['dim_referer'], "")
        self.assertEqual(stored_hits[8]['dim_language'], "el-GR,el;q=0.9,en;q=0.8")
        self.assertEqual(stored_hits[8]['dim_os'], "Android")
        self.assertEqual(stored_hits[8]['dim_zone'], "756262")
        self.assertEqual(stored_hits[8]['dim_external_id'], "crub")
        self.assertEqual(stored_hits[8]['campaign'], 0)
        self.assertEqual(stored_hits[8]['destination'], 2)
        self.assertEqual(list(sorted(filter(lambda k: 'dim_' in k, stored_hits[8].keys()))), ['dim_connection_type', 'dim_creative_id', 'dim_external_id', 'dim_ip', 'dim_keywords', 'dim_langcode', 'dim_language', 'dim_os', 'dim_os_version', 'dim_referer', 'dim_ua_category', 'dim_ua_name', 'dim_ua_type', 'dim_ua_vendor', 'dim_ua_version', 'dim_useragent', 'dim_zone'])



    def test_import_dynamic_entity_hits_incrementally(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)

        self.import_redis_fixture(first_import_hits_fixture)
        data_import.load_hits()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Hits', ENTITIES['Hits']), (8, 9))

        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        self.assertEqual(len(stored_hits), 9)

        self.import_redis_fixture(add_hits_fixture)
        data_import.load_hits()
        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        # should import all new objects
        self.assertEqual(len(stored_hits), 24)

# fixture_data['Hits:[22]'] = '{"time":{"secs_since_epoch":1550533112,"nanos_since_epoch":207362916},"campaign_id":"Campaign:[0]","destination_id":"Offer:[2]","click_id":"121507283048865792","cost":{"value":85,"currency":"USD"},"dimensions":{"useragent":"Mozilla/5.0 (Linux; Android 8.0.0; SM-A750FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","ua_category":"smartphone","external_id":"c05s","os":"Android","os_version":"8.0.0","connection_type":"BROADBAND","langcode":"en-GB","ua_type":"browser","keywords":"","language":"en-GB,en-US;q=0.9,en;q=0.8","ua_vendor":"Google","zone":"847358","ip":"127.0.0.1","creative_id":"","ua_name":"Chrome","ua_version":"72.0.3626.105"}}'
        self.assertEqual(stored_hits[22]['time'], datetime.utcfromtimestamp(1550533112))
        self.assertEqual(stored_hits[22]['click_id'], '121507283048865792')
        self.assertEqual(stored_hits[22]['cost'], Decimal('0.00085'))
        self.assertEqual(stored_hits[22]['dim_referer'], "")
        self.assertEqual(stored_hits[22]['dim_language'], "en-GB,en-US;q=0.9,en;q=0.8")
        self.assertEqual(stored_hits[22]['dim_os'], "Android")
        self.assertEqual(stored_hits[22]['dim_zone'], "847358")
        self.assertEqual(stored_hits[22]['dim_external_id'], "c05s")
        self.assertEqual(stored_hits[22]['campaign'], 0)
        self.assertEqual(stored_hits[22]['destination'], 2)
        self.assertEqual(list(sorted(filter(lambda k: 'dim_' in k, stored_hits[22].keys()))), ['dim_connection_type', 'dim_creative_id', 'dim_external_id', 'dim_ip', 'dim_keywords', 'dim_langcode', 'dim_language', 'dim_os', 'dim_os_version', 'dim_referer', 'dim_ua_category', 'dim_ua_name', 'dim_ua_type', 'dim_ua_vendor', 'dim_ua_version', 'dim_useragent', 'dim_zone'])

# fixture_data['Hits:[23]'] = '{"time":{"secs_since_epoch":1550533116,"nanos_since_epoch":354360280},"campaign_id":"Campaign:[0]","destination_id":"Offer:[2]","click_id":"121507300232929280","cost":{"value":229,"currency":"USD"},"dimensions":{"ua_vendor":"Google","connection_type":"XDSL","creative_id":"","keywords":"","langcode":"el-GR","os_version":"7.0","ua_type":"browser","language":"el-GR,el;q=0.9","ip":"127.0.0.1","referer":"http://newstarads.com/afu.php?zoneid=1407888&var=1407888","os":"Android","zone":"1407888","ua_version":"72.0.3626.105","useragent":"Mozilla/5.0 (Linux; Android 7.0; ZTE BLADE A602) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","external_id":"c0dy","ua_name":"Chrome","ua_category":"smartphone"}}'
        self.assertEqual(stored_hits[23]['time'], datetime.utcfromtimestamp(1550533116))
        self.assertEqual(stored_hits[23]['click_id'], '121507300232929280')
        self.assertEqual(stored_hits[23]['cost'], Decimal('0.00229'))
        self.assertEqual(stored_hits[23]['dim_referer'], "http://newstarads.com/afu.php?zoneid=1407888&var=1407888")
        self.assertEqual(stored_hits[23]['dim_language'], "el-GR,el;q=0.9")
        self.assertEqual(stored_hits[23]['dim_os'], "Android")
        self.assertEqual(stored_hits[23]['dim_zone'], "1407888")
        self.assertEqual(stored_hits[23]['dim_external_id'], "c0dy")
        self.assertEqual(stored_hits[23]['campaign'], 0)
        self.assertEqual(stored_hits[23]['destination'], 2)
        self.assertEqual(list(sorted(filter(lambda k: 'dim_' in k, stored_hits[23].keys()))), ['dim_connection_type', 'dim_creative_id', 'dim_external_id', 'dim_ip', 'dim_keywords', 'dim_langcode', 'dim_language', 'dim_os', 'dim_os_version', 'dim_referer', 'dim_ua_category', 'dim_ua_name', 'dim_ua_type', 'dim_ua_vendor', 'dim_ua_version', 'dim_useragent', 'dim_zone'])


    def test_import_dynamic_entity_with_automigration(self):
        self.data_import = data_import = DataImport(bus=self.bus, report_db=self.report_db)

        self.import_redis_fixture(first_import_hits_fixture)
        data_import.load_hits()

        self.assertEqual(self.data_import.get_idx_of_latest_saved_entity('Hits', ENTITIES['Hits']), (8, 9))

        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        self.assertEqual(len(stored_hits), 9)

        self.import_redis_fixture(add_hits_with_automigration_fixture)
        data_import.load_hits()
        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]
        # should import all new objects
        self.assertEqual(len(stored_hits), 24)

# fixture_data['Hits:[22]'] = '{"time":{"secs_since_epoch":1550533112,"nanos_since_epoch":207362916},"campaign_id":"Campaign:[0]","destination_id":"Offer:[2]","click_id":"121507283048865792","cost":{"value":85,"currency":"USD"},"dimensions":{"useragent":"Mozilla/5.0 (Linux; Android 8.0.0; SM-A750FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","ua_category":"smartphone","external_id":"c05s","os":"Android","os_version":"8.0.0","connection_type":"BROADBAND","langcode":"en-GB","ua_type":"browser","keywords":"","language":"en-GB,en-US;q=0.9,en;q=0.8","ua_vendor":"Google","zone":"847358","ip":"127.0.0.1","creative_id":"","ua_name":"Chrome","ua_version":"72.0.3626.105"}}'
        self.assertEqual(stored_hits[22]['time'], datetime.utcfromtimestamp(1550533112))
        self.assertEqual(stored_hits[22]['click_id'], '121507283048865792')
        self.assertEqual(stored_hits[22]['cost'], Decimal('0.00085'))
        self.assertEqual(stored_hits[22]['dim_referer'], "")
        self.assertEqual(stored_hits[22]['dim_language'], "en-GB,en-US;q=0.9,en;q=0.8")
        self.assertEqual(stored_hits[22]['dim_os'], "Android")
        self.assertEqual(stored_hits[22]['dim_zone'], "847358")
        self.assertEqual(stored_hits[22]['dim_external_id'], "c05s")
        self.assertEqual(stored_hits[22]['campaign'], 0)
        self.assertEqual(stored_hits[22]['destination'], 2)
        self.assertEqual(stored_hits[22]['dim_another_dimension'], '')
        self.assertEqual(stored_hits[22]['dim_new_dimension'], '')
        self.assertEqual(stored_hits[9]['dim_new_dimension'], 'sometestvalue')
        self.assertEqual(stored_hits[19]['dim_another_dimension'], 'anothertestvalue')
        self.assertEqual(list(sorted(filter(lambda k: 'dim_' in k, stored_hits[22].keys()))), ['dim_another_dimension', 'dim_connection_type', 'dim_creative_id', 'dim_external_id', 'dim_ip', 'dim_keywords', 'dim_langcode', 'dim_language', 'dim_new_dimension', 'dim_os', 'dim_os_version', 'dim_referer', 'dim_ua_category', 'dim_ua_name', 'dim_ua_type', 'dim_ua_vendor', 'dim_ua_version', 'dim_useragent', 'dim_zone'])

        self.import_redis_fixture(add_hits_with_missed_fields_fixture)
        data_import.load_hits()
        stored_hits = zip(*list(self.report_db.connected().read(sql="select * from test.hits;",
                                                                columns=self.report_db.connected().describe(table='hits'))))[0]

        self.assertEqual(stored_hits[24]['dim_another_dimension'], '')
        self.assertEqual(stored_hits[25]['dim_new_dimension'], '')

    def test_import_with_missed_objects(self):
        pass

if __name__ == '__main__':
    unittest.main()
