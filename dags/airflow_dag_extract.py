from extract import *
from transform import *
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='extraction_and_loading_minio',
    default_args=default_args,
    description='Scraping des livres, chargement dans minio et transformation avec spark',
    start_date=datetime(2026, 4, 26),
    schedule='@daily',   # Exécution chaque jour
    catchup=False
) as dag:

    scraping_task = PythonOperator(
        task_id='scrape_books',
        python_callable=scrap_and_upload
    )

    transformation_task = PythonOperator(
        task_id='transformation',
        python_callable=transformation
    )

    scraping_task >> transformation_task