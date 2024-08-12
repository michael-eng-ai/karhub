import unittest
import mysql.connector
import pandas as pd
from unittest.mock import patch, MagicMock
from src.load import create_connection, create_tables, load_to_db, load_data

class TestLoad(unittest.TestCase):

    @patch('src.load.mysql.connector.connect')
    def test_create_connection(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        result = create_connection()
        self.assertEqual(result, mock_conn)
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3307,
            user="root",
            password="1234",
            database="karhub_db"
        )

    @patch('src.load.mysql.connector.connect')
    def test_create_tables(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        create_tables(mock_conn)

        self.assertEqual(mock_cursor.execute.call_count, 3)  # Three CREATE TABLE statements (despesas, receitas, budget_data)
        mock_conn.commit.assert_called_once()

    @patch('src.load.load_to_db')
    @patch('src.load.pd.DataFrame.to_sql')
    def test_load_to_db(self, mock_to_sql, mock_load_to_db):
        mock_conn = MagicMock()
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        
        load_to_db(mock_conn, df, 'test_table')

        mock_to_sql.assert_called_once_with('test_table', mock_conn, if_exists='replace', index=False)

    @patch('src.load.create_connection')
    @patch('src.load.create_tables')
    @patch('src.load.load_to_db')
    def test_load_data(self, mock_load_to_db, mock_create_tables, mock_create_connection):
        mock_conn = MagicMock()
        mock_create_connection.return_value = mock_conn

        transformed_data = {
            'despesas': pd.DataFrame({'ID Fonte Recurso': ['001'], 'Nome Fonte Recurso': ['Teste'], 'Liquidado': [100.0]}),
            'receitas': pd.DataFrame({'ID Fonte Recurso': ['001'], 'Nome Fonte Recurso': ['Teste'], 'Arrecadado': [200.0]}),
            'currency_rate': 5.0
        }

        load_data(transformed_data)

        mock_create_connection.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        self.assertEqual(mock_load_to_db.call_count, 3)  # Called for despesas, receitas, and budget_data
        mock_conn.close.assert_called_once()

    @patch('src.load.create_connection')
    def test_load_data_connection_error(self, mock_create_connection):
        mock_create_connection.return_value = None

        transformed_data = {
            'despesas': pd.DataFrame({'ID Fonte Recurso': ['001'], 'Nome Fonte Recurso': ['Teste'], 'Liquidado': [100.0]}),
            'receitas': pd.DataFrame({'ID Fonte Recurso': ['001'], 'Nome Fonte Recurso': ['Teste'], 'Arrecadado': [200.0]}),
            'currency_rate': 5.0
        }

        with self.assertRaises(Exception):
            load_data(transformed_data)

if __name__ == '__main__':
    unittest.main()