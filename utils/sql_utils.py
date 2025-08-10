import os
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import json

def get_sql_path(layer, domain=None, table_type=None, table_name=None):
    """
    Get the path to SQL file based on the layer, domain, table type and table name
    
    Args:
        layer (str): Data layer (raw, core, datamart)
        domain (str, optional): Domain (e.g., sales, marketing)
        table_type (str, optional): Table type for core layer (dim, fact)
        table_name (str): Name of the table
        
    Returns:
        str: Full path to the SQL file
    """
    base_path = Path(__file__).parents[1] / "sql"
    
    if layer.lower() == "raw":
        if domain:
            return str(base_path / "raw" / domain / f"{table_name}.sql")
        else:
            return str(base_path / "raw" / f"{table_name}.sql")
    
    elif layer.lower() == "core":
        if table_type:
            return str(base_path / "core" / table_type / f"{table_name}.sql")
        else:
            return str(base_path / "core" / f"{table_name}.sql")
    
    elif layer.lower() == "datamart":
        if domain:
            return str(base_path / "datamart" / domain / f"{table_name}.sql")
        else:
            return str(base_path / "datamart" / f"{table_name}.sql")
    
    else:
        raise ValueError(f"Unknown layer: {layer}")

def read_sql_file(file_path, **kwargs):
    """
    Read SQL file and replace placeholders with provided values
    
    Args:
        file_path (str): Path to the SQL file
        **kwargs: Variables to replace in the SQL query
        
    Returns:
        str: SQL query with replaced variables
    """
    with open(file_path, 'r') as f:
        query = f.read()
    
    # Replace variables
    for key, value in kwargs.items():
        query = query.replace(f"{{{{{key}}}}}", str(value))
    
    return query

def load_config(config_file):
    """
    Load YAML configuration file
    
    Args:
        config_file (str): Path to the YAML config file
        
    Returns:
        dict: Configuration as dictionary
    """
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def get_table_config(domain, table_name=None):
    """
    Get configuration for a specific table or all tables in a domain
    
    Args:
        domain (str): Data domain (e.g., sales, marketing)
        table_name (str, optional): Specific table name
        
    Returns:
        dict: Table configuration
    """
    config_path = Path(__file__).parents[1] / "config" / f"{domain}_config.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    config = load_config(str(config_path))
    
    if table_name:
        if table_name in config.get('tables', {}):
            return config['tables'][table_name]
        else:
            raise ValueError(f"Table {table_name} not found in config")
    
    return config
