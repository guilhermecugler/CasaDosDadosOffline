import unittest
from threading import Event
from utils.get_cnpj_numbers import get_cnpj_numbers_sqlite

class TestGetCnpjNumbers(unittest.TestCase):
    def test_get_cnpj_numbers_sqlite(self):
        json_filters = {
            'query': {'termo': ['teste'], 'uf': ['SP']},
            'range_query': {'data_abertura': {'gte': '2023-01-01'}},
            'extras': {'com_email': True},
            'page': 1
        }
        cnpjs = get_cnpj_numbers_sqlite(json_filters, lambda x: None, lambda x: None, Event())
        self.assertIsInstance(cnpjs, list)
        if cnpjs:
            self.assertTrue(all(isinstance(cnpj, str) for cnpj in cnpjs))

if __name__ == '__main__':
    unittest.main()
