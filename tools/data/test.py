import unittest
from decimal import Decimal


from framework.test import assert_data_object_cls, assert_reporting_object_cls, assert_reporting_object_instance, create_fake_entity

from model import *


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
        fake_campaign = create_fake_entity(Campaign,
                                           entity_name='Campaign',
                                           idx=0,

                                           name='Test campaign',
                                           alias='testalias',
                                           paused_offers=[],
                                           offers=[],

                                           optimize=False,
                                           optimization_paused=True,
                                           hit_limit_for_optimization=50,
                                           slicing_attrs=[]
                                           )
        fake_offer = create_fake_entity(Offer, entity_name='Offer', idx=0)
        fake_conversion = create_fake_entity(Conversion,
                                             entity_name='Conversions',
                                             idx=0,
                                             external_id='abc',
                                             revenue={'value':Decimal(0.05), 'currency': 'USD'},
                                             status='lead',
                                             time={'secs_since_epoch': 10000000}
                                             )

        fake_hit = create_fake_entity(Hit,
                                      entity_name='Hits',
                                      idx=0,
                                      dimensions={'test_dimension': 'dimension_value'},
                                      campaign_id='Campaign:[0]',
                                      destination_id='Offer:[0]',
                                      cost={'value':Decimal(0.001), 'currency': 'USD'},
                                      time={'secs_since_epoch': 10000000}
                                      )

        assert_reporting_object_instance(self, fake_campaign)
        assert_reporting_object_instance(self, fake_offer)
        assert_reporting_object_instance(self, fake_conversion)
        assert_reporting_object_instance(self, fake_hit)


if __name__ == '__main__':
    unittest.main()
