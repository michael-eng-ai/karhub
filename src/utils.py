import yaml
from typing import Dict, Any
import logging

def load_config(config_path: str = 'config/config.yaml') -> Dict[str, Any]:
    """
    Load configuration from a YAML file.
    """
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        raise

def setup_logging(log_level: str = 'INFO'):
    """
    Set up logging configuration.
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def validate_dataframe(df, required_columns):
    """
    Validate that a dataframe has the required columns.
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Dataframe is missing required columns: {missing_columns}")

if __name__ == '__main__':
    # Test the functions
    config = load_config()
    print("Config loaded successfully:")
    print(config)

    setup_logging()
    logging.info("Logging set up successfully")

    import pandas as pd
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    try:
        validate_dataframe(df, ['A', 'B', 'C'])
    except ValueError as e:
        print(f"Validation error (expected): {e}")