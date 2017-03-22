import unittest

import descarteslabs as dl


class TestRuncible(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = dl.metadata

    def test_sources(self):
        r = self.instance.sources()
        self.assertGreater(len(r), 0)

    def test_search(self):
        r = self.instance.search()
        self.assertEqual(len(r['features']), 1000)

    @unittest.skip("search by sat_id not currently supported")
    def test_sat_id(self):
        r = self.instance.search(start_time='2016-09-01', end_time='2016-09-02', sat_id='LANDSAT_8')
        self.assertEqual(100, len(r['features']))

    def test_const_id(self):
        r = self.instance.search(start_time='2016-09-01', end_time='2016-09-02', const_id=['L8'])
        self.assertGreater(len(r['features']), 0)

    @unittest.skip("search by shape name not currently supported")
    def test_shape(self):
        r = self.instance.search(shape='north-america_united-states_iowa', limit=1)
        self.assertEqual(1, len(r['features']))

    def test_summary(self):
        r = self.instance.summary(start_time='2016-09-01', end_time='2016-09-02', const_id=['L8'])
        self.assertEqual(1, len(list(r)))


if __name__ == '__main__':
    unittest.main()
