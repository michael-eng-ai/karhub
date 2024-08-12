import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import Dict, Any
from datetime import datetime

def create_bigquery_client():
    return bigquery.Client()

def create_tables(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    
    # Tabela de despesas
    table_ref = dataset_ref.table("despesas")
    schema = [
        bigquery.SchemaField("id_fonte_recurso", "STRING"),
        bigquery.SchemaField("nome_fonte_recurso", "STRING"),
        bigquery.SchemaField("total_liquidado", "FLOAT"),
        bigquery.SchemaField("dt_insert", "TIMESTAMP")
    ]
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        client.get_table(table_ref)
        print(f"Tabela {table_ref.table_id} já existe.")
    except NotFound:
        client.create_table(table)
        print(f"Tabela {table_ref.table_id} criada.")

    # Tabela de receitas
    table_ref = dataset_ref.table("receitas")
    schema = [
        bigquery.SchemaField("id_fonte_recurso", "STRING"),
        bigquery.SchemaField("nome_fonte_recurso", "STRING"),
        bigquery.SchemaField("total_arrecadado", "FLOAT"),
        bigquery.SchemaField("dt_insert", "TIMESTAMP")
    ]
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        client.get_table(table_ref)
        print(f"Tabela {table_ref.table_id} já existe.")
    except NotFound:
        client.create_table(table)
        print(f"Tabela {table_ref.table_id} criada.")

    # Tabela de budget_data
    table_ref = dataset_ref.table("budget_data")
    schema = [
        bigquery.SchemaField("id_fonte_recurso", "STRING"),
        bigquery.SchemaField("nome_fonte_recurso", "STRING"),
        bigquery.SchemaField("total_liquidado", "FLOAT"),
        bigquery.SchemaField("total_arrecadado", "FLOAT"),
        bigquery.SchemaField("dt_insert", "TIMESTAMP")
    ]
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        client.get_table(table_ref)
        print(f"Tabela {table_ref.table_id} já existe.")
    except NotFound:
        client.create_table(table)
        print(f"Tabela {table_ref.table_id} criada.")

def load_to_bigquery(client, df: pd.DataFrame, dataset_id: str, table_name: str):
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    df['dt_insert'] = datetime.now()

    # Carregar o DataFrame para a tabela no BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Espera o job ser concluído
    
    print(f"Carregamento concluído para a tabela {table_id}.")

def load_data(transformed_data: Dict[str, Any], dataset_id: str):
    client = create_bigquery_client()
    create_tables(client, dataset_id)
    
    # Carregar dados transformados nas tabelas "despesas" e "receitas"
    load_to_bigquery(client, transformed_data['despesas'], dataset_id, 'despesas')
    load_to_bigquery(client, transformed_data['receitas'], dataset_id, 'receitas')
    
    # Agrupar e combinar os dados
    despesas_grouped = transformed_data['despesas'].groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({
        'Liquidado': 'sum'
    }).reset_index()
    
    receitas_grouped = transformed_data['receitas'].groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({
        'Arrecadado': 'sum'
    }).reset_index()
    
    final_df = pd.merge(despesas_grouped, receitas_grouped, on=['ID Fonte Recurso', 'Nome Fonte Recurso'], how='outer').fillna(0)
    
    # Converter as colunas conforme necessário (exemplo: USD para BRL)
    final_df['Total Liquidado'] = final_df['Liquidado'] * transformed_data['currency_rate']
    final_df['Total Arrecadado'] = final_df['Arrecadado'] * transformed_data['currency_rate']
    
    # Carregar o DataFrame final na tabela "budget_data"
    load_to_bigquery(client, final_df, dataset_id, 'budget_data')
    print("Todos os dados foram carregados com sucesso.")

if __name__ == '__main__':
    from transform import transform_data
    from extract import extract_data
    
    dataset_id = "seu_dataset"  # Substitua pelo ID do seu dataset no BigQuery
    
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    
    load_data(transformed_data, dataset_id)