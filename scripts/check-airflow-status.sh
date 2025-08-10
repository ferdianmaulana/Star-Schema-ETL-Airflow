#!/bin/bash
# Helper script to check Airflow DAG and script status on GCE

# Set variables
AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
AIRFLOW_USER=${AIRFLOW_USER:-airflow}

echo "Airflow Script Status Checker"
echo "============================"

# Check Airflow service status
echo "Checking Airflow services..."
if systemctl is-active --quiet airflow-webserver; then
    echo "✅ Airflow webserver is running"
else
    echo "❌ Airflow webserver is not running"
fi

if systemctl is-active --quiet airflow-scheduler; then
    echo "✅ Airflow scheduler is running"
else
    echo "❌ Airflow scheduler is not running"
fi

# Check DAG folder
echo -e "\nChecking DAG files..."
DAG_COUNT=$(find $AIRFLOW_HOME/dags -name "*.py" | wc -l)
echo "Found $DAG_COUNT DAG files"

# List DAGs from Airflow
echo -e "\nListing available DAGs in Airflow..."
sudo -u $AIRFLOW_USER airflow dags list

# Check for errors in DAG files
echo -e "\nChecking for syntax errors in DAG files..."
for dag_file in $(find $AIRFLOW_HOME/dags -name "*.py"); do
    echo "Checking $dag_file"
    python -m py_compile $dag_file 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ Error in $dag_file"
    else
        echo "✅ $dag_file is valid"
    fi
done

# Check SQL files
echo -e "\nChecking SQL files..."
SQL_COUNT=$(find $AIRFLOW_HOME/dags/sql -name "*.sql" | wc -l)
echo "Found $SQL_COUNT SQL files"

echo -e "\nDeployment Status: Complete"
echo "For more details, check the Airflow webserver UI"
echo "To restart Airflow services, run: /usr/local/bin/restart-airflow"
