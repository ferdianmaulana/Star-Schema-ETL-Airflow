"""
DAG to process data from raw to core layer (dimension and fact tables)
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from plugins.ms_teams_notification import MSTeamsNotificationPlugin

# Import from project root instead of dags directory
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.operators import BigQueryExecuteOperator
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
core_dataset = config.get('core_dataset', f'core_{DOMAIN}')

# Create the DAG
dag = DAG(
    f'process_core_{DOMAIN}',
    default_args=default_args,
    description=f'Process {DOMAIN} data from raw to core layer',
    schedule_interval='0 2 * * *',  # Run at 02:00 every day
    catchup=False,
    max_active_runs=1,
    tags=[f'{DOMAIN}', 'core', 'processing'],
)

start = DummyOperator(task_id='start', dag=dag)

# Wait for the raw data ingestion to complete
wait_for_raw_ingestion = ExternalTaskSensor(
    task_id='wait_for_raw_ingestion',
    external_dag_id=f'ingest_raw_{DOMAIN}',
    external_task_id='end',  # Wait for the end task of the raw ingestion DAG
    mode='reschedule',  # Reschedule if the task isn't complete yet
    timeout=3600,  # Timeout after 1 hour
    poke_interval=60,  # Check every minute
    dag=dag,
)

dim_tables_complete = DummyOperator(task_id='dim_tables_complete', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Process dimension tables first
dim_tasks = {}
for table, table_config in config.get('tables', {}).items():
    if table_config.get('type') == 'dimension':
        # Parameters for SQL query with execution date as DSTART
        params = {
            'project_id': PROJECT_ID,
            'raw_dataset': raw_dataset,
            'core_dataset': core_dataset,
            'dstart': '{{ ds }}',  # Use Airflow's execution date in YYYY-MM-DD format
        }
        
        # SQL file path
        sql_path = get_sql_path('core', table_type='dim', table_name=table)
        
        # Create processing task
        dim_task = BigQueryExecuteOperator(
            task_id=f'process_{table}',
            sql_file_path=sql_path,
            destination_dataset_table=f'{PROJECT_ID}.{core_dataset}.{table}',
            write_disposition='WRITE_APPEND',  # Append for SCD Type 2
            create_disposition='CREATE_IF_NEEDED',
            params=params,
            dag=dag,
        )
        
        dim_tasks[table] = dim_task
        # Set dependencies
        start >> wait_for_raw_ingestion >> dim_task >> dim_tables_complete

# Process fact tables after dimensions
for table, table_config in config.get('tables', {}).items():
    if table_config.get('type') == 'fact':
        # Parameters for SQL query with execution date as DSTART
        params = {
            'project_id': PROJECT_ID,
            'raw_dataset': raw_dataset,
            'core_dataset': core_dataset,
            'dstart': '{{ ds }}',  # Use Airflow's execution date in YYYY-MM-DD format
        }
        
        # SQL file path
        sql_path = get_sql_path('core', table_type='fact', table_name=table)
        
        # Create processing task
        fact_task = BigQueryExecuteOperator(
            task_id=f'process_{table}',
            sql_file_path=sql_path,
            destination_dataset_table=f'{PROJECT_ID}.{core_dataset}.{table}',
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            params=params,
            dag=dag,
        )
        
        # Set dependencies - fact tables depend on all dimension tables
        dim_tables_complete >> fact_task >> end
