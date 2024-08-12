import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.extract import load_config, read_csv, get_currency_rate, extract_data

class TestExtract(unittest.TestCase):

    @patch('src.extract.yaml.safe_load')
    def test_load_config(self, mock_safe_load):
        mock_safe_load.return_value = {'test': 'config'}
        result = load_config()
        self.assertEqual(result, {'test': 'config'})

    @patch('src.extract.pd.read_csv')
    def test_read_csv(self, mock_read_csv):
        mock_df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_read_csv.return_value = mock_df
        result = read_csv('test.csv')
        pd.testing.assert_frame_equal(result, mock_df)

    @patch('src.extract.requests.get')
    def test_get_currency_rate(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'high': '5.5', 'low': '5.4', 'bid': '5.45', 'ask': '5.46', 'timestamp': '1655251200'}]
        mock_get.return_value = mock_response

        result = get_currency_rate('USD-BRL', '20230515')
        self.assertEqual(result['high'], 5.5)
        self.assertEqual(result['low'], 5.4)
        self.assertEqual(result['bid'], 5.45)
        self.assertEqual(result['ask'], 5.46)

    @patch('src.extract.load_config')
    @patch('src.extract.read_csv')
    @patch('src.extract.get_currency_rate')
    def test_extract_data(self, mock_get_currency_rate, mock_read_csv, mock_load_config):
        mock_load_config.return_value = {
            'input': {'despesas_file': 'despesas.csv', 'receitas_file': 'receitas.csv'},
            'api': {'currency_pair': 'USD-BRL', 'target_date': '2023-05-15'}
        }
        mock_read_csv.side_effect = [
            pd.DataFrame({'Despesa': [100, 200]}),
            pd.DataFrame({'Receita': [300, 400]})
        ]
        mock_get_currency_rate.return_value = {'high': 5.5, 'low': 5.4, 'bid': 5.45, 'ask': 5.46}

        result = extract_data()

        self.assertIn('despesas', result)
        self.assertIn('receitas', result)
        self.assertIn('currency_data', result)
        self.assertEqual(result['currency_data']['high'], 5.5)
        self.assertEqual(result['currency_data']['bid'], 5.45)

if __name__ == '__main__':
    unittest.main()