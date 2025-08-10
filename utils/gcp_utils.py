import os
from google.oauth2 import service_account
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

def get_gcp_credentials():
    """
    Get GCP credentials from environment variables or Airflow connections.
    This allows for flexible credential management in different environments.
    """
    # Check if service account is set in environment variable
    service_account_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    
    if service_account_json:
        # Return credentials directly from environment variable
        return service_account.Credentials.from_service_account_info(
            eval(service_account_json)
        )
    else:
        # Alternatively get from Airflow connections
        connection = BaseHook.get_connection('google_cloud_default')
        keyfile_dict = eval(connection.extra_dejson.get('keyfile_dict', '{}'))
        
        if keyfile_dict:
            return service_account.Credentials.from_service_account_info(keyfile_dict)
        
        # Fallback to application default credentials
        return None

def get_project_id():
    """
    Get GCP project ID from environment variables or Airflow variables.
    """
    # Try to get from environment variables first
    project_id = os.environ.get('GCP_PROJECT_ID')
    
    # If not available, try Airflow variables
    if not project_id:
        project_id = Variable.get('gcp_project_id', default_var=None)
    
    return project_id

def get_bigquery_client():
    """
    Get authenticated BigQuery client
    """
    from google.cloud import bigquery
    
    credentials = get_gcp_credentials()
    project_id = get_project_id()
    
    if credentials:
        return bigquery.Client(credentials=credentials, project=project_id)
    else:
        return bigquery.Client(project=project_id)

def get_gcs_client():
    """
    Get authenticated GCS client
    """
    from google.cloud import storage
    
    credentials = get_gcp_credentials()
    project_id = get_project_id()
    
    if credentials:
        return storage.Client(credentials=credentials, project=project_id)
    else:
        return storage.Client(project=project_id)
