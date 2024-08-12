import logging
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.utils import load_config, setup_logging

def main():
    # Etapa de Extração
    extracted_data = extract_data()

    # Etapa de Transformação
    transformed_data = transform_data(extracted_data)
    
    # Etapa de Carregamento
    load_data(transformed_data)

if __name__ == '__main__':
    main()