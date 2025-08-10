#!/bin/bash
# This script prepares a GCE instance for updating Airflow scripts via CI/CD
# Assumes Airflow is already installed and running

# Set variables
AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
AIRFLOW_USER=${AIRFLOW_USER:-airflow}
AIRFLOW_GROUP=${AIRFLOW_GROUP:-airflow}

echo "Setting up GCE instance for Airflow script updates"
echo "Using AIRFLOW_HOME: $AIRFLOW_HOME"

# Make sure the directory structure exists
sudo mkdir -p $AIRFLOW_HOME/dags
sudo mkdir -p $AIRFLOW_HOME/dags/sql
sudo mkdir -p $AIRFLOW_HOME/dags/sql/raw
sudo mkdir -p $AIRFLOW_HOME/dags/sql/core
sudo mkdir -p $AIRFLOW_HOME/dags/sql/datamart
sudo mkdir -p $AIRFLOW_HOME/dags/config
sudo mkdir -p $AIRFLOW_HOME/dags/utils
sudo mkdir -p $AIRFLOW_HOME/backups

# Set up the correct permissions
sudo chown -R $AIRFLOW_USER:$AIRFLOW_GROUP $AIRFLOW_HOME

# Create a basic script to restart Airflow services
cat << 'EOF' | sudo tee /usr/local/bin/restart-airflow
#!/bin/bash
# Script to restart Airflow services

# Determine how Airflow is installed and running
if systemctl is-active --quiet airflow-webserver; then
    # Systemd services
    sudo systemctl restart airflow-webserver
    sudo systemctl restart airflow-scheduler
elif [ -f ~/airflow/airflow-webserver.pid ]; then
    # Running as processes
    kill $(cat ~/airflow/airflow-webserver.pid)
    kill $(cat ~/airflow/airflow-scheduler.pid)
    
    # Start again
    airflow webserver -D
    airflow scheduler -D
else
    echo "Cannot determine how to restart Airflow. Please restart manually."
fi
EOF

sudo chmod +x /usr/local/bin/restart-airflow

echo "GCE instance setup complete."
echo "The CI/CD pipeline will now be able to update Airflow scripts."
echo "Use /usr/local/bin/restart-airflow to restart Airflow services when needed."
