import unittest
from utils.get_cnpj_data import get_all_cnpj_data_sqlite

class TestGetAllCnpjData(unittest.TestCase):
    def test_get_all_cnpj_data_sqlite(self):
        json_filters = {
            'query': {'termo': ['teste'], 'uf': ['SP']},
            'range_query': {'data_abertura': {'gte': '2023-01-01'}},
            'extras': {'com_email': True},
            'page': 1
        }
        data = get_all_cnpj_data_sqlite(json_filters, lambda x: None)
        self.assertIsInstance(data, list)
        if data:
            self.assertIsInstance(data[0], dict)

if __name__ == '__main__':
    unittest.main()
