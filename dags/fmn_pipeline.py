from airflow import DAG  #Verify that the DAG import is correct or we use airflow.models.DAG or go to my dev container
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Let's import from the etl_ingest for raw data
from ingest_data.etl_ingest import ingest_all_sheets

# Default arguments applied to all tasks
default_args = {
    'owner': 'CodeWithEniola',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,  #check submission requirement
}

def load_raw_data(**context):
    """
    Task function that calls your ingest_all_sheets() function.
    """
    # Use the container path (mounted from ./dags/dataset)
    file_path = '/opt/airflow/dags/dataset/FMN Data Engineer Assesment Dataset.xlsx'  #revisit before submission, I had to change the path to match my container structure
    
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Excel file not found at: {file_path}")
    
    # Call YOUR function exactly as you wrote it
    sheets = ['Transactions', 'Products', 'Distributors', 
              'Salespersons', 'Monthly_Targets', 'Date_Table']
    
    result = ingest_all_sheets(file_path, sheets)
    
    context['ti'].xcom_push(key='row_counts', value=result)
    return f"Loaded {result} rows"

def create_dbt_schemas():
    """Create staging and marts schemas for dbt."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Schemas 'staging' and 'marts' created (or already exist)")

with DAG(
    dag_id='fmn_sales_pipeline',
    default_args=default_args,
    description='ELT pipeline: Load raw data, transform with dbt, test quality',
    schedule='@daily',  #Check submission requirement, done - I don't have that. - Done
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['fmn', 'elt', 'dbt'],
) as dag:

    create_schemas = PythonOperator(
        task_id='create_schemas',
        python_callable=create_dbt_schemas,
    )

    extract_task = PythonOperator(
        task_id='load_raw_excel',
        python_callable=load_raw_data
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dags/fmn_dbt && dbt run --profiles-dir .',
        env={
            'DBT_HOST': 'postgres',
            'DBT_USER': 'airflow',
            'DBT_PASSWORD': 'airflow',
            'DBT_PORT': '5432',
            'DBT_DBNAME': 'airflow',
            'DBT_SCHEMA': 'staging',
        },
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dags/fmn_dbt && dbt test --profiles-dir .',
        env={
            'DBT_HOST': 'postgres',
            'DBT_USER': 'airflow',
            'DBT_PASSWORD': 'airflow',
            'DBT_PORT': '5432',
            'DBT_DBNAME': 'airflow',
            'DBT_SCHEMA': 'staging',
        },
    )

    create_schemas >> extract_task >> dbt_run >> dbt_test