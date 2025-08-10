#!/bin/bash
# Helper script to create a new data domain structure

# Check if domain name is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <domain_name>"
  echo "Example: $0 marketing"
  exit 1
fi

DOMAIN=$1
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Creating directory structure for domain: $DOMAIN"

# Create SQL directories
mkdir -p "$PROJECT_DIR/dags/sql/raw/$DOMAIN"
mkdir -p "$PROJECT_DIR/dags/sql/datamart/$DOMAIN"

# Create config file
cat > "$PROJECT_DIR/dags/config/${DOMAIN}_config.yaml" << EOL
# ${DOMAIN^} domain configuration
project_id: "your-gcp-project-id"
raw_dataset: "raw_${DOMAIN}"
core_dataset: "core_${DOMAIN}"
datamart_dataset: "datamart_${DOMAIN}"

# Default parameters
default:
  location: "US"
  partition_field: "date"
  
# Source information
sources:
  gcs:
    bucket: "raw-data-bucket"
    file_format: "CSV"
    
# Tables configuration
tables:
  # Add your tables here
EOL

# Create DAG files
cat > "$PROJECT_DIR/dags/ingest_raw_${DOMAIN}.py" << EOL
"""
DAG to ingest data from GCS to BigQuery raw layer for ${DOMAIN} domain
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

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
}

# Domain for this DAG
DOMAIN = '${DOMAIN}'

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
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=[f'{DOMAIN}', 'raw', 'ingestion'],
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Add your tasks here
# start >> your_tasks >> end
EOL

echo "Created directory structure and template files for $DOMAIN domain"
echo "Next steps:"
echo "1. Update the config file: dags/config/${DOMAIN}_config.yaml"
echo "2. Create SQL files in: dags/sql/raw/$DOMAIN/"
echo "3. Update the DAG file: dags/ingest_raw_${DOMAIN}.py"
