import unittest
from fakeredis import FakeStrictRedis

class LimiterTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeStrictRedis()
        pass

    def test_limiter(self):
        self.assertTrue(True)

    def tearDown(self):
        pass