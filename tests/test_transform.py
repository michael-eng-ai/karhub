import unittest
import pandas as pd
import numpy as np
from src.transform import transform_despesas, transform_receitas, apply_currency_conversion, transform_data

class TestTransform(unittest.TestCase):

    def test_transform_despesas(self):
        input_df = pd.DataFrame({
            'Fonte de Recursos': ['001 - TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR', '002 - OUTRAS FONTES'],
            'Liquidado': ['10.000,00', '20.000,00']
        })
        result = transform_despesas(input_df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ['Liquidado', 'ID Fonte Recurso', 'Nome Fonte Recurso', 'Liquidado_USD'])
        self.assertEqual(result['ID Fonte Recurso'].iloc[0], '001')
        self.assertEqual(result['Nome Fonte Recurso'].iloc[0], 'TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR')
        self.assertAlmostEqual(result['Liquidado'].iloc[0], 10000.00)

    def test_transform_receitas(self):
        input_df = pd.DataFrame({
            'Fonte de Recursos': ['001 - TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR', '002 - OUTRAS FONTES'],
            'Arrecadado': ['30.000,00', '40.000,00']
        })
        result = transform_receitas(input_df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ['Arrecadado', 'ID Fonte Recurso', 'Nome Fonte Recurso', 'Arrecadado_USD'])
        self.assertEqual(result['ID Fonte Recurso'].iloc[0], '001')
        self.assertEqual(result['Nome Fonte Recurso'].iloc[0], 'TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR')
        self.assertAlmostEqual(result['Arrecadado'].iloc[0], 30000.00)

    def test_apply_currency_conversion(self):
        input_df = pd.DataFrame({
            'Valor': [100, 200, 300]
        })
        currency_rate = 5.0
        result = apply_currency_conversion(input_df, currency_rate, 'Valor')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('Valor_USD', result.columns)
        np.testing.assert_array_almost_equal(result['Valor_USD'], [20, 40, 60])

    def test_transform_data(self):
        input_data = {
            'despesas': pd.DataFrame({
                'Fonte de Recursos': ['001 - TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR', '002 - OUTRAS FONTES'],
                'Liquidado': ['10.000,00', '20.000,00']
            }),
            'receitas': pd.DataFrame({
                'Fonte de Recursos': ['001 - TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR', '002 - OUTRAS FONTES'],
                'Arrecadado': ['30.000,00', '40.000,00']
            }),
            'currency_data': {'bid': 5.0}
        }
        result = transform_data(input_data)
        self.assertIsInstance(result, dict)
        self.assertIn('despesas', result)
        self.assertIn('receitas', result)
        self.assertIn('currency_rate', result)
        self.assertEqual(result['currency_rate'], 5.0)
        self.assertIn('Liquidado_USD', result['despesas'].columns)
        self.assertIn('Arrecadado_USD', result['receitas'].columns)

if __name__ == '__main__':
    unittest.main()