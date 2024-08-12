import pandas as pd
import requests
import yaml
from typing import Dict, Any
from datetime import datetime

def load_config() -> Dict[str, Any]:
    """Carrega o arquivo de configuração YAML."""
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def read_csv(file_path: str) -> pd.DataFrame:
    """Lê um arquivo CSV e retorna um DataFrame pandas."""
    return pd.read_csv(file_path, encoding='latin1', thousands='.', decimal=',')

def get_currency_rate(currency_pair: str, date: str) -> Dict[str, Any]:
    """
    Obtém a cotação de uma moeda para uma data específica.
    
    Args:
        currency_pair (str): Par de moedas (e.g., 'USD-BRL').
        date (str): Data no formato 'YYYYMMDD'.
    
    Returns:
        dict: Dados da cotação, incluindo valores de alta, baixa, compra, venda e timestamp.
    """
    base_url = "https://economia.awesomeapi.com.br/json/daily"
    url = f"{base_url}/{currency_pair}/1?start_date={date}&end_date={date}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data:
            return {
                'high': float(data[0]['high']),
                'low': float(data[0]['low']),
                'bid': float(data[0]['bid']),
                'ask': float(data[0]['ask']),
                'timestamp': datetime.fromtimestamp(int(data[0]['timestamp']))
            }
        else:
            raise ValueError(f"No data available for {currency_pair} on {date}")
    else:
        raise ConnectionError(f"Failed to fetch currency data: {response.status_code}")

def extract_data() -> Dict[str, Any]:
    """
    Extrai dados dos arquivos CSV e da API de cotação.
    
    Returns:
        dict: Contém DataFrames das despesas e receitas, e os dados de cotação.
    """
    config = load_config()
    
    despesas_df = read_csv(config['input']['despesas_file'])
    receitas_df = read_csv(config['input']['receitas_file'])
    
    currency_pair = config['api']['currency_pair']
    target_date = config['api']['target_date']
    formatted_date = datetime.strptime(target_date, '%Y-%m-%d').strftime('%Y%m%d')
    
    currency_data = get_currency_rate(currency_pair, formatted_date)
    
    return {
        'despesas': despesas_df,
        'receitas': receitas_df,
        'currency_data': currency_data
    }

if __name__ == '__main__':
    extracted_data = extract_data()
    print("Data extracted successfully.")
    print(f"Despesas shape: {extracted_data['despesas'].shape}")
    print(f"Receitas shape: {extracted_data['receitas'].shape}")
    print(f"Currency data: {extracted_data['currency_data']}")