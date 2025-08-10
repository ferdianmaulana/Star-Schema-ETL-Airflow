#!/bin/bash
# Script to deploy ETL scripts to Airflow server

# Enable debug output
set -x

# Define variables (these will be overridden by CI/CD environment variables)
AIRFLOW_HOME=${AIRFLOW_HOME:-/srv/airflow}
CLONE_PATH=${CLONE_PATH:-/tmp/etl-deploy}
BRANCH=${BRANCH:-main}

echo "Deploying to Airflow server..."
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "CLONE_PATH: $CLONE_PATH"
echo "BRANCH: $BRANCH"

# Refresh previous temp folder
rm -rf ${CLONE_PATH}
mkdir -p ${CLONE_PATH}

# Clone the repository if this script is run directly (not needed in CI/CD as code is already checked out)
if [ ! -d "./dags" ]; then
  echo "Cloning repository..."
  git clone $REPO_URL --branch $BRANCH --single-branch ${CLONE_PATH}
  cd ${CLONE_PATH}
fi

# Build docker image for ETL processes
echo "Building Docker image..."
docker build -t star-schema-etl:${BRANCH} .

# Delete dangling images
echo "Cleaning up dangling images..."
dangling_images=$(docker images --filter "dangling=true" -q --no-trunc)
if [ -n "$dangling_images" ]; then 
  docker rmi $dangling_images
fi

# Replace any CI/CD variables in DAG definitions
echo "Updating DAG variables..."
find ./dags/ -type f -name "*.py" -exec sed -i -e "s|<BRANCH>|$BRANCH|g" {} \;

# Create backup of existing DAGs
echo "Creating backup of existing DAGs..."
timestamp=$(date +%Y%m%d%H%M%S)
mkdir -p ${AIRFLOW_HOME}/backups/${timestamp}
cp -r ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/backups/${timestamp}/ || echo "No existing dags to backup"

# Sync dags folder
echo "Syncing DAGs folder..."
rsync -av ./dags/ ${AIRFLOW_HOME}/dags

# Sync SQL folder structure
echo "Syncing SQL folder structure..."
mkdir -p ${AIRFLOW_HOME}/dags/sql/raw
mkdir -p ${AIRFLOW_HOME}/dags/sql/core/dim
mkdir -p ${AIRFLOW_HOME}/dags/sql/core/fact
mkdir -p ${AIRFLOW_HOME}/dags/sql/datamart
rsync -av ./dags/sql/ ${AIRFLOW_HOME}/dags/sql

# Sync config folder
echo "Syncing config folder..."
mkdir -p ${AIRFLOW_HOME}/dags/config
rsync -av ./dags/config/ ${AIRFLOW_HOME}/dags/config

# Sync utils folder
echo "Syncing utils folder..."
mkdir -p ${AIRFLOW_HOME}/dags/utils
rsync -av ./dags/utils/ ${AIRFLOW_HOME}/dags/utils

# Sync plugins folder if it exists
if [ -d "./plugins" ]; then
  echo "Syncing plugins folder..."
  mkdir -p ${AIRFLOW_HOME}/plugins
  rsync -av --delete ./plugins/ ${AIRFLOW_HOME}/plugins
fi

# Set proper permissions
echo "Setting permissions..."
sudo chown -R airflow:airflow ${AIRFLOW_HOME}/dags
sudo chown -R airflow:airflow ${AIRFLOW_HOME}/plugins

# Restart Airflow services
echo "Restarting Airflow services..."
if command -v systemctl &> /dev/null && systemctl is-active --quiet airflow-scheduler; then
  sudo systemctl restart airflow-webserver
  sudo systemctl restart airflow-scheduler
else
  echo "Airflow not running as systemd service, please restart manually."
fi

echo "Deployment completed successfully!"
