import unittest

import descarteslabs as dl


class TestPlaces(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = dl.places

    def test_placetypes(self):
        data = self.instance.placetypes()
        self.assertEqual(6, len(data))

    def test_find(self):
        r = self.instance.find('united-states_iowa')
        self.assertEqual(1, len(r))

    def test_shape(self):
        r = self.instance.shape('north-america_united-states_iowa')
        self.assertEqual(85688713, r['id'])

    def test_prefix(self):
        # counties by default
        r = self.instance.prefix('north-america_united-states_iowa')
        self.assertEqual(99, len(r['features']))

        r = self.instance.prefix('north-america_united-states_iowa', placetype='district')
        self.assertEqual(9, len(r['features']))

    def test_sources(self):
        r = self.instance.sources()
        self.assertEqual(2, len(r))

    def test_categories(self):
        r = self.instance.categories()
        self.assertEqual(2, len(r))

    def test_metrics(self):
        r = self.instance.metrics()
        self.assertEqual(3, len(r))

    def test_triples(self):
        r = self.instance.triples()
        self.assertEqual(12, len(r))

    def test_statistics(self):
        r = self.instance.statistics('north-america_united-states')
        self.assertEqual(782, len(r))


if __name__ == '__main__':
    unittest.main()
