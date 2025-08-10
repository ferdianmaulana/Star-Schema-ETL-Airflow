"""
DAG to orchestrate data ingestion from GCS to BigQuery raw layer
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from plugins.ms_teams_notification import MSTeamsNotificationPlugin

# Import from project root instead of dags directory
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.operators import GCSBigQueryOperator
from utils.sql_utils import get_table_config, get_sql_path
from utils.gcp_utils import get_project_id

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': MSTeamsNotificationPlugin.notify_failure,
    'on_success_callback': MSTeamsNotificationPlugin.notify_success,
}

# Domain for this DAG
DOMAIN = 'sales'

# Get project ID from environment
PROJECT_ID = get_project_id()

# Load domain configuration
config = get_table_config(DOMAIN)
raw_dataset = config.get('raw_dataset', f'raw_{DOMAIN}')

# Create the DAG
dag = DAG(
    f'ingest_raw_{DOMAIN}',
    default_args=default_args,
    description=f'Ingest {DOMAIN} data from GCS to BigQuery raw layer',
    schedule_interval='0 0 * * *',  # Run at 00:00 (midnight) every day
    catchup=False,
    max_active_runs=1,
    tags=[f'{DOMAIN}', 'raw', 'ingestion'],
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Create tasks for each table in the domain
tables = [table for table, config in config.get('tables', {}).items() 
          if config.get('source', {}).get('type') == 'gcs']

for table in tables:
    table_config = config['tables'][table]
    source_config = table_config.get('source', {})
    schema = table_config.get('schema', [])
    
    # Get GCS path
    gcs_path = source_config.get('path')
    
    # Ensure path includes domain and date parameters
    if DOMAIN not in gcs_path:
        # Add domain to path if not present
        gcs_path = f"{DOMAIN}/{gcs_path}"
    
    # Ensure path contains date parameter
    if '{date}' not in gcs_path:
        # Extract the base path (everything before the last '/')
        base_path = '/'.join(gcs_path.split('/')[:-1])
        file_name = gcs_path.split('/')[-1]
        
        # Add date parameter
        gcs_path = f"{base_path}/{{date}}/{file_name.split('.')[0]}_{{date}}.{file_name.split('.')[-1]}"
    
    # Substitute the date parameter with the execution date
    gcs_path = gcs_path.replace('{date}', '{{ ds_nodash }}')  # YYYYMMDD format
    
    bucket = config.get('sources', {}).get('gcs', {}).get('bucket')
    
    # Ensure partition field is set
    partition_field = table_config.get('partition_field', 'ingestion_timestamp')
    
    # Create a load task for this table
    load_task = GCSBigQueryOperator(
        task_id=f'load_{table}',
        source_bucket=bucket,
        source_object=gcs_path,
        destination_project_dataset_table=f'{PROJECT_ID}.{raw_dataset}.{table}',
        schema_fields=schema,
        source_format=config.get('sources', {}).get('gcs', {}).get('file_format', 'CSV'),
        write_disposition='WRITE_APPEND',  # Changed to APPEND for backfill compatibility
        time_partitioning={"field": partition_field},
        dag=dag,
    )
    
    # Set up dependencies
    start >> load_task >> end
