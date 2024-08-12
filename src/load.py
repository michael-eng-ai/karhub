import pandas as pd
import mysql.connector
from typing import Dict, Any
from datetime import datetime

def create_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",  
            port=3307, 
            user="root",
            password="1234",
            database="karhub_db"
        )
        return conn
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS despesas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        id_fonte_recurso VARCHAR(50),
        nome_fonte_recurso VARCHAR(255),
        total_liquidado DECIMAL(20, 2),
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS receitas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        id_fonte_recurso VARCHAR(50),
        nome_fonte_recurso VARCHAR(255),
        total_arrecadado DECIMAL(20, 2),
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS budget_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        id_fonte_recurso VARCHAR(50),
        nome_fonte_recurso VARCHAR(255),
        total_liquidado DECIMAL(20, 2),
        total_arrecadado DECIMAL(20, 2),
        dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    conn.commit()

def load_to_db(conn, df: pd.DataFrame, table_name: str):
    cursor = conn.cursor()
    df['dt_insert'] = datetime.now()

    for _, row in df.iterrows():
        if table_name == 'despesas':
            cursor.execute('''
                INSERT INTO despesas (id_fonte_recurso, nome_fonte_recurso, total_liquidado, dt_insert)
                VALUES (%s, %s, %s, %s)
            ''', (row['ID Fonte Recurso'], row['Nome Fonte Recurso'], row['Liquidado'], row['dt_insert']))
        elif table_name == 'receitas':
            cursor.execute('''
                INSERT INTO receitas (id_fonte_recurso, nome_fonte_recurso, total_arrecadado, dt_insert)
                VALUES (%s, %s, %s, %s)
            ''', (row['ID Fonte Recurso'], row['Nome Fonte Recurso'], row['Arrecadado'], row['dt_insert']))
        elif table_name == 'budget_data':
            cursor.execute('''
                INSERT INTO budget_data (id_fonte_recurso, nome_fonte_recurso, total_liquidado, total_arrecadado, dt_insert)
                VALUES (%s, %s, %s, %s, %s)
            ''', (row['ID Fonte Recurso'], row['Nome Fonte Recurso'], row['Total Liquidado'], row['Total Arrecadado'], row['dt_insert']))
    
    conn.commit()

def load_data(transformed_data: Dict[str, Any]):
    conn = create_connection()
    if conn is not None:
        create_tables(conn)
        
        # Load individual transformed data into despesas and receitas tables
        load_to_db(conn, transformed_data['despesas'], 'despesas')
        load_to_db(conn, transformed_data['receitas'], 'receitas')
        
        # Aggregating the total liquidated and total collected amounts by 'ID Fonte Recurso' and 'Nome Fonte Recurso'
        despesas_grouped = transformed_data['despesas'].groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({
            'Liquidado': 'sum'
        }).reset_index()
        
        receitas_grouped = transformed_data['receitas'].groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({
            'Arrecadado': 'sum'
        }).reset_index()
        
        # Merging the two dataframes on 'ID Fonte Recurso' and 'Nome Fonte Recurso'
        final_df = pd.merge(despesas_grouped, receitas_grouped, on=['ID Fonte Recurso', 'Nome Fonte Recurso'], how='outer').fillna(0)

        # Convert from USD to BRL
        final_df['Total Liquidado'] = final_df['Liquidado'] * transformed_data['currency_rate']
        final_df['Total Arrecadado'] = final_df['Arrecadado'] * transformed_data['currency_rate']

        # Load aggregated data into budget_data table
        load_to_db(conn, final_df, 'budget_data')

        conn.close()
        print("Data loaded successfully.")
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    from transform import transform_data
    from extract import extract_data
    
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    
    load_data(transformed_data)