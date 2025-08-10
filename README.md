# Star-Schema-ETL-Airflow

## Overview

This project implements a scalable data pipeline using Apache Airflow to perform ETL (Extract, Transform, Load) operations from Google Cloud Storage to BigQuery, organizing data in a star schema with multiple layers:

1. **Raw Layer**: Direct ingestion from source systems (GCS) to BigQuery
2. **Core Layer**: Organized as dimension (SCD Type 2) and fact tables
3. **Datamart Layer**: Pre-aggregated analytics views for business users

## Project Structure

```
Star-Schema-ETL-Airflow/
├── config/               # Configuration files
│   └── sales_config.yaml
├── dags/                 # Airflow DAGs
│   ├── ingest_raw_sales.py
│   ├── process_core_sales.py
│   └── process_datamart_sales.py
├── plugins/              # Airflow plugins
│   ├── ms_teams_notification.py   # MS Teams notification plugin
│   ├── ms_teams_webhook_hook.py   # MS Teams webhook hook
│   └── ms_teams_webhook_operator.py # MS Teams webhook operator
├── scripts/              # Setup and deployment scripts
│   ├── deploy.sh         # Deployment script
│   ├── check-airflow-status.sh # Status checking script
│   └── setup-gce-instance.sh # GCE instance setup script
├── sql/                  # SQL queries organized by layer
│   ├── raw/              # Raw data ingestion 
│   │   └── sales/
│   ├── core/             # Star schema (dimensions and facts)
│   │   ├── dim/
│   │   └── fact/
│   └── datamart/         # Business-specific aggregations
│       └── sales/
├── utils/                # Utility modules
│   ├── gcp_utils.py      # GCP connection utilities
│   ├── operators.py      # Custom Airflow operators
│   └── sql_utils.py      # SQL file handling utilities
├── .github/              # GitHub workflows
│   └── workflows/
│       └── gce-deploy.yml # CI/CD workflow file
├── Dockerfile            # For building utility image
├── requirements.txt      # Python dependencies
└── README.md
```

## Data Flow

The pipeline follows this sequence:
1. **Ingestion**: Raw data from GCS is loaded into BigQuery raw tables
2. **Transformation**:
   - **Dimensions**: SCD Type 2 dimensions are created/updated with appropriate tracking
   - **Facts**: Fact tables are created by joining with dimension tables
3. **Analytics**: Datamart views are created from core layer for business use cases

## DAGs

The project contains the following DAGs:

1. **ingest_raw_sales**: Ingests data from GCS to BigQuery raw layer
2. **process_core_sales**: Processes data from raw to core (dimensions and facts)
3. **process_datamart_sales**: Processes data from core to datamart

## Data Profile

This ETL pipeline is designed to handle a sales data domain with the following structure:

### GCS File Structure
```
gs://{bucket_name}/{domain}/{table_name}/{YYYYMMDD}/{table_name}_{YYYYMMDD}.csv
```

Example:
```
gs://my-data-bucket/
├── sales/                           # Domain
│   ├── customers/                   # Table name
│   │   ├── 20230101/                # Date partition
│   │   │   └── customers_20230101.csv
│   │   └── 20230102/
│   │       └── customers_20230102.csv
│   ├── products/
│   │   └── ...
│   └── orders/
│       └── ...
```

### Data Tables

#### Raw Layer
- **customers**: Customer information (id, name, contact details, etc.)
- **products**: Product catalog (id, name, category, price, etc.)
- **orders**: Order header information (id, customer_id, date, amount, status)
- **order_items**: Order line items (order_id, product_id, quantity, price)

#### Core Layer (Star Schema)
- **Dimensions**:
  - **dim_customers** (SCD Type 2): Tracks historical changes to customer information
  - **dim_products** (SCD Type 2): Tracks historical changes to product information
  - **dim_dates**: Date dimension with various date attributes

- **Facts**:
  - **fact_orders**: Transactional-level fact table with line item details

#### Datamart Layer
- **sales_summary**: Daily sales aggregated by product category
- **customer_analytics**: Customer-level metrics with RFM segmentation

### Key Design Features

1. **Time-Based Partitioning**:
   - All tables are partitioned by date for efficient querying
   - Raw tables use `ingestion_timestamp` as the partition field
   - Fact tables use `order_date` as the partition field

2. **SCD Type 2 Implementation**:
   - Tracks historical changes to dimensions with:
     - Surrogate keys (customer_sk, product_sk)
     - Effective date ranges (effective_date, expiration_date)
     - Current record flag (is_current)

3. **Backfill Support**:
   - All DAGs use execution date ({{ ds }}) for filtering
   - Pipeline supports running for historical dates
   - Append-based loading strategy preserves historical data

4. **Point-in-Time Joins**:
   - Fact tables join to dimensions based on their effective dates
   - Ensures historical accuracy in reporting

## Key Features

- **SCD Type 2 Implementation**: Proper tracking of historical dimension changes
- **Service Account Authentication**: Uses service account from environment variables
- **Scalable Design**: Easy to add new domains, tables, or metrics
- **Configurable**: YAML-based configuration for tables and schemas
- **Idempotent**: Each step can be re-run without duplication
- **Backfill-Compatible**: All processes support date-based backfilling

## Setup and Configuration

### Prerequisites

- Apache Airflow
- Google Cloud Platform account with BigQuery and GCS access
- Service account with appropriate permissions

### Environment Variables

Set the following environment variables:
- `GCP_PROJECT_ID`: Your GCP project ID
- `GCP_SERVICE_ACCOUNT_JSON`: Service account credentials in JSON format

### Configuration Files

Edit the YAML files in the `dags/config` directory to configure:
- Source data locations
- Table schemas
- Processing parameters

## Adding New Domains

To add a new data domain:

1. Create a new configuration file (e.g., `marketing_config.yaml`)
2. Add SQL files for each layer
3. Create new DAGs following the pattern in existing DAGs
4. Update dependencies as needed

## Running the Pipeline

The pipeline DAGs are scheduled to run at specific times, with ExternalTaskSensors ensuring proper sequencing:

1. **ingest_raw_sales** runs at 00:00 (midnight) every day
2. **process_core_sales** runs at 02:00, and automatically waits for ingest_raw_sales to complete
3. **process_datamart_sales** runs at 04:00, and automatically waits for process_core_sales to complete

This staggered scheduling gives each stage time to complete before the next one is scheduled to run, while the ExternalTaskSensors ensure that each stage waits for its dependencies to finish successfully regardless of timing.

You can also trigger individual DAGs manually:
```bash
airflow dags trigger ingest_raw_sales
```

## Deployment Options

### Production Deployment with CI/CD

This project includes CI/CD setup for deploying DAG scripts to an existing Airflow installation:

1. **Prerequisites**:
   - Airflow already installed and running on a server
   - SSH access configured for GitHub Actions

2. **Environment Setup**:
   ```bash
   # SSH into your server and run
   bash scripts/setup-gce-instance.sh
   ```

3. **Configure GitHub Secrets**:
   Set up the following secrets in your GitHub repository:
   - `GCP_PROJECT_ID`: Your Google Cloud project ID
   - `GCP_SA_KEY`: Service account key with permissions for authentication
   - `AIRFLOW_PROD_IP`: IP address of your production Airflow server
   - `AIRFLOW_STAGING_IP`: IP address of your staging Airflow server (optional)
   - `SSH_PRIVATE_KEY`: SSH private key for accessing your Airflow servers

4. **Deployment Process**:
   The CI/CD pipeline follows these steps:
   - Builds the Docker image locally for testing
   - Connects to your Airflow server via SSH
   - Clones the repository on the server
   - Syncs all DAGs, SQL files, config files, and utilities
   - Sets proper permissions
   - Restarts Airflow services

5. **Manual Deployment**:
   You can also deploy manually using the deployment script:
   ```bash
   # Run locally or on the server
   bash scripts/deploy.sh
   ```

6. **Multiple Environments**:
   - Push to `main` branch for automatic production deployment
   - Use the manual workflow trigger to deploy to staging
   - Select the environment from the workflow dispatch menu