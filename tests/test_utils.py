import unittest
import yaml
import logging
import pandas as pd
from unittest.mock import patch, mock_open
from io import StringIO
from src.utils import load_config, setup_logging, validate_dataframe

class TestUtils(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="database:\n  path: /path/to/db")
    def test_load_config(self, mock_file):
        config = load_config()
        self.assertIsInstance(config, dict)
        self.assertIn('database', config)
        self.assertEqual(config['database']['path'], '/path/to/db')

    @patch("builtins.open", new_callable=mock_open)
    def test_load_config_file_not_found(self, mock_file):
        mock_file.side_effect = FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            load_config()

    @patch("builtins.open", new_callable=mock_open, read_data="invalid: yaml: content")
    def test_load_config_invalid_yaml(self, mock_file):
        with patch('yaml.safe_load', side_effect=yaml.YAMLError):
            with self.assertRaises(yaml.YAMLError):
                load_config()

    @patch('logging.basicConfig')
    def test_setup_logging(self, mock_basic_config):
        setup_logging('DEBUG')
        mock_basic_config.assert_called_once()
        args, kwargs = mock_basic_config.call_args
        self.assertEqual(kwargs['level'], logging.DEBUG)

    def test_setup_logging_invalid_level(self):
        with self.assertRaises(ValueError):
            setup_logging('INVALID_LEVEL')

    def test_validate_dataframe(self):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
        validate_dataframe(df, ['A', 'B', 'C'])  # This should not raise an exception

    def test_validate_dataframe_missing_columns(self):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        with self.assertRaises(ValueError):
            validate_dataframe(df, ['A', 'B', 'C'])

if __name__ == '__main__':
    unittest.main()