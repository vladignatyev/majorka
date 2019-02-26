import unittest
import sys

print sys.path

from ..bus import Connection

class DatabusTestCase(unittest.TestCase):
    def setUp(self):
        print "set up"
        print "unittest" in sys.modules
        pass

    def tearDown(self):
        print "tear down"
        pass
    def test_simple(self):
        self.assertEqual(1,1, "ok")
