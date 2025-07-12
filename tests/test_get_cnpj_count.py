import unittest
from utils.get_cnpj_count import get_cnpj_count_sqlite

class TestGetCnpjCount(unittest.TestCase):
    def test_get_cnpj_count_sqlite(self):
        json_filters = {
            'query': {'termo': ['teste'], 'uf': ['SP']},
            'range_query': {'data_abertura': {'gte': '2023-01-01'}},
            'extras': {'com_email': True},
            'page': 1
        }
        count = get_cnpj_count_sqlite(json_filters, lambda x: None)
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)

if __name__ == '__main__':
    unittest.main()
