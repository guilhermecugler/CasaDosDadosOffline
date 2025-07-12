import unittest
from threading import Event
from utils.get_cnpj_data import get_cnpj_data_sqlite

class TestGetCnpjData(unittest.TestCase):
    def test_get_cnpj_data_sqlite(self):
        cnpjs = ["12345678000195"]  # Substitua por um CNPJ válido do seu banco de teste
        file_name = "test_output.xlsx"
        count = get_cnpj_data_sqlite(cnpjs, file_name, lambda x: None, Event())
        self.assertIsInstance(count, int)
        import os
        if count > 0:
            self.assertTrue(os.path.exists(file_name))
            os.remove(file_name)

if __name__ == '__main__':
    unittest.main()
