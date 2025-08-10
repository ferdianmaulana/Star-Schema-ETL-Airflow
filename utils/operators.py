from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
from google.cloud import storage
import logging
from utils.gcp_utils import get_bigquery_client, get_gcs_client
from utils.sql_utils import read_sql_file

class GCSBigQueryOperator(BaseOperator):
    """
    Operator to load data from GCS to BigQuery.
    Supports various file formats and loading configurations.
    """
    
    @apply_defaults
    def __init__(
            self,
            source_bucket,
            source_object,
            destination_project_dataset_table,
            schema_fields=None,
            source_format='CSV',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            field_delimiter=',',
            skip_leading_rows=0,
            allow_quoted_newlines=False,
            allow_jagged_rows=False,
            max_bad_records=0,
            ignore_unknown_values=False,
            *args, 
            **kwargs):
        
        super(GCSBigQueryOperator, self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.skip_leading_rows = skip_leading_rows
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.max_bad_records = max_bad_records
        self.ignore_unknown_values = ignore_unknown_values
        
    def execute(self, context):
        logging.info(f"Loading data from GCS bucket {self.source_bucket} object {self.source_object} to BigQuery table {self.destination_project_dataset_table}")
        
        bq_client = get_bigquery_client()
        
        job_config = bigquery.LoadJobConfig()
        
        # Set job configuration
        job_config.source_format = getattr(bigquery.SourceFormat, self.source_format)
        job_config.create_disposition = getattr(bigquery.CreateDisposition, self.create_disposition)
        job_config.write_disposition = getattr(bigquery.WriteDisposition, self.write_disposition)
        
        if self.schema_fields:
            job_config.schema = [bigquery.SchemaField.from_api_repr(f) for f in self.schema_fields]
            
        if self.source_format == 'CSV':
            job_config.skip_leading_rows = self.skip_leading_rows
            job_config.field_delimiter = self.field_delimiter
            job_config.allow_quoted_newlines = self.allow_quoted_newlines
            job_config.allow_jagged_rows = self.allow_jagged_rows
            
        job_config.max_bad_records = self.max_bad_records
        job_config.ignore_unknown_values = self.ignore_unknown_values
        
        uri = f"gs://{self.source_bucket}/{self.source_object}"
        
        # Start the load job
        load_job = bq_client.load_table_from_uri(
            uri,
            self.destination_project_dataset_table,
            job_config=job_config
        )
        
        # Wait for job to complete
        load_job.result()
        
        # Get loaded table
        table = bq_client.get_table(self.destination_project_dataset_table)
        
        logging.info(f"Loaded {table.num_rows} rows into {self.destination_project_dataset_table}")
        
        return {'rows_loaded': table.num_rows}


class BigQueryExecuteOperator(BaseOperator):
    """
    Operator to execute SQL queries in BigQuery.
    Can read from SQL files and substitute parameters.
    """
    
    @apply_defaults
    def __init__(
            self,
            sql=None,
            sql_file_path=None,
            destination_dataset_table=None,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            use_legacy_sql=False,
            location='US',
            params=None,
            *args, 
            **kwargs):
        
        super(BigQueryExecuteOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.sql_file_path = sql_file_path
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.params = params or {}
        
    def execute(self, context):
        if self.sql_file_path:
            self.sql = read_sql_file(self.sql_file_path, **self.params)
            
        if not self.sql:
            raise ValueError("Either sql or sql_file_path must be provided")
            
        logging.info(f"Executing SQL query: {self.sql[:100]}...")
        
        bq_client = get_bigquery_client()
        
        job_config = bigquery.QueryJobConfig()
        job_config.use_legacy_sql = self.use_legacy_sql
        
        if self.destination_dataset_table:
            job_config.destination = self.destination_dataset_table
            job_config.write_disposition = getattr(bigquery.WriteDisposition, self.write_disposition)
            job_config.create_disposition = getattr(bigquery.CreateDisposition, self.create_disposition)
            
        # Start the query job
        query_job = bq_client.query(
            self.sql,
            job_config=job_config,
            location=self.location
        )
        
        # Wait for job to complete
        results = query_job.result()
        
        if hasattr(results, 'total_rows'):
            logging.info(f"Query returned {results.total_rows} rows")
            
        if self.destination_dataset_table:
            table = bq_client.get_table(self.destination_dataset_table)
            logging.info(f"Loaded {table.num_rows} rows into {self.destination_dataset_table}")
            
        return {'job_id': query_job.job_id}
