import unittest

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
        assert_reporting_object_instance(self, create_fake_entity(Campaign, entity_name='Campaign', idx=0))
        assert_reporting_object_instance(self, create_fake_entity(Offer, entity_name='Offer', idx=0))
        assert_reporting_object_instance(self, create_fake_entity(Conversion, entity_name='Conversions', idx=0))
        assert_reporting_object_instance(self, create_fake_entity(Hit, entity_name='Hits', idx=0, dimensions={}))


if __name__ == '__main__':
    unittest.main()
