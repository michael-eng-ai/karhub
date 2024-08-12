import pandas as pd
from typing import Dict, Any

def transform_despesas(df: pd.DataFrame) -> pd.DataFrame:
    # Remover colunas desnecessárias
    df = df.drop(columns=[col for col in df.columns if 'Unnamed' in col], errors='ignore')

    # Normalizar e converter a coluna 'Liquidado' para valores numéricos
    df['Liquidado'] = pd.to_numeric(df['Liquidado']
                                    .astype(str)
                                    .str.replace(' ', '')
                                    .str.replace('.', '')
                                    .str.replace(',', '.'),
                                    errors='coerce').fillna(0)
    
    # Separar informações da coluna 'Fonte de Recursos'
    df[['ID Fonte Recurso', 'Nome Fonte Recurso']] = df['Fonte de Recursos'].str.split(' - ', n=1, expand=True)
    
    # Substituir valores NaN por "não-identificado"
    df['ID Fonte Recurso'].fillna('000', inplace=True)
    df['Nome Fonte Recurso'].fillna('não-identificado', inplace=True)
    
    # Garantir a conversão dos tipos de dados adequados
    df = df.astype({
        'ID Fonte Recurso': 'str',
        'Nome Fonte Recurso': 'str',
        'Liquidado': 'float'
    })
    
    return df

def transform_receitas(df: pd.DataFrame) -> pd.DataFrame:
    # Remover colunas desnecessárias
    df = df.drop(columns=[col for col in df.columns if 'Unnamed' in col], errors='ignore')

    # Normalizar e converter a coluna 'Arrecadado' para valores numéricos
    df['Arrecadado'] = pd.to_numeric(df['Arrecadado']
                                     .astype(str)
                                     .str.replace(' ', '')
                                     .str.replace('.', '')
                                     .str.replace(',', '.'),
                                     errors='coerce').fillna(0)
    
    # Separar informações da coluna 'Fonte de Recursos'
    df[['ID Fonte Recurso', 'Nome Fonte Recurso']] = df['Fonte de Recursos'].str.split(' - ', n=1, expand=True)
    
    df['ID Fonte Recurso'] = df['ID Fonte Recurso'].fillna('não-identificado')
    df['Nome Fonte Recurso'] = df['Nome Fonte Recurso'].fillna('não-identificado')
    
    # Garantir a conversão dos tipos de dados adequados
    df = df.astype({
        'ID Fonte Recurso': 'str',
        'Nome Fonte Recurso': 'str',
        'Arrecadado': 'float'
    })
    
    return df

def apply_currency_conversion(df: pd.DataFrame, currency_rate: float, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[f'{column}_USD'] = df[column] / currency_rate
    else:
        raise KeyError(f"A coluna '{column}' não foi encontrada no DataFrame.")
    return df

def transform_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    despesas_df = transform_despesas(extracted_data['despesas'])
    receitas_df = transform_receitas(extracted_data['receitas'])
    
    currency_rate = extracted_data['currency_data']['bid']  # Usando a taxa de compra
    
    despesas_df = apply_currency_conversion(despesas_df, currency_rate, 'Liquidado')
    receitas_df = apply_currency_conversion(receitas_df, currency_rate, 'Arrecadado')
    
    return {
        'despesas': despesas_df,
        'receitas': receitas_df,
        'currency_rate': currency_rate
    }

if __name__ == '__main__':
    from extract import extract_data
    
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    
    print("Data transformed successfully.")
    print(f"Despesas shape: {transformed_data['despesas'].shape}")
    print(f"Receitas shape: {transformed_data['receitas'].shape}")
    print(f"Currency rate used: {transformed_data['currency_rate']}")