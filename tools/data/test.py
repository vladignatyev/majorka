import unittest
import os

from decimal import Decimal
from redis import Redis

from framework.bus import Connection as BusConnection
from framework.test.asserts import assert_data_object_cls, assert_reporting_object_cls, assert_reporting_object_instance, create_fake_entity

from model import *
from test.fixtures.first_import import fixture_data as first_import_fixture
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


    def setUp(self):
        self.redis.flushdb()
        self.report_db.connected()
        self.report_db.drop()

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
        self.assertEqual(len(hits_to_save), 9)


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

    def test_import_conversions_incrementally(self):
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



if __name__ == '__main__':
    unittest.main()
