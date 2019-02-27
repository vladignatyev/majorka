import unittest
from decimal import Decimal


from framework.test.asserts import assert_data_object_cls, assert_reporting_object_cls, assert_reporting_object_instance, create_fake_entity

from model import *


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


if __name__ == '__main__':
    unittest.main()
