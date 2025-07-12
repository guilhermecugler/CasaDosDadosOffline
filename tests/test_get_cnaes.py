import unittest
from utils.get_cnae import get_cnaes

class TestGetCnaes(unittest.TestCase):
    def test_get_cnaes(self):
        cnaes, codes = get_cnaes()
        self.assertIsInstance(cnaes, list)
        self.assertIsInstance(codes, list)
        self.assertEqual(len(cnaes), len(codes))
        self.assertEqual(cnaes[0], 'Todas Atividades')
        self.assertEqual(codes[0], '')

if __name__ == '__main__':
    unittest.main()