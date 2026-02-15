"""
DAG: Walmart E-commerce Pipeline
Downloads Walmart E-commerce dataset from Kaggle and uploads to MinIO
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import zipfile
import logging

# Конфигурация
KAGGLE_DATASET = "alexpaul9959/dataset-walmart"
LOCAL_PATH = "/tmp/walmart_data"
MINIO_BUCKET = "walmart-raw"
MINIO_CONN_ID = "minio_conn"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def download_from_kaggle(**context):
    """
    Скачивает датасет с Kaggle и распаковывает
    """

    # Устанавливаем credentials из Variables
    os.environ['KAGGLE_USERNAME'] = Variable.get('kaggle_username')
    os.environ['KAGGLE_KEY'] = Variable.get('kaggle_key')

    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    # Создаём директорию
    os.makedirs(LOCAL_PATH, exist_ok=True)

    # Скачиваем датасет
    logging.info(f"Downloading dataset: {KAGGLE_DATASET}")
    api.dataset_download_files(
        KAGGLE_DATASET,
        path=LOCAL_PATH,
        unzip=True
    )

    # Получаем список CSV файлов
    csv_files = [f for f in os.listdir(LOCAL_PATH) if f.endswith('.csv')]
    logging.info(f"Downloaded {len(csv_files)} CSV files: {csv_files}")

    # Передаём список файлов в следующий task через XCom
    return csv_files


def upload_to_minio(**context):
    """
    Загружает CSV файлы в MinIO
    """
    # Получаем список файлов из предыдущего task
    csv_files = context['task_instance'].xcom_pull(task_ids='download_from_kaggle')

    # Подключаемся к MinIO через S3Hook
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # Создаём bucket если не существует
    if not s3_hook.check_for_bucket(MINIO_BUCKET):
        s3_hook.create_bucket(bucket_name=MINIO_BUCKET)
        logging.info(f"Created bucket: {MINIO_BUCKET}")

    uploaded_files = []

    for csv_file in csv_files:
        local_file_path = os.path.join(LOCAL_PATH, csv_file)
        s3_key = f"raw/{csv_file}"

        logging.info(f"Uploading {csv_file} to s3://{MINIO_BUCKET}/{s3_key}")

        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=MINIO_BUCKET,
            replace=True
        )

        uploaded_files.append(s3_key)
        logging.info(f"Successfully uploaded: {csv_file}")

    logging.info(f"Total uploaded: {len(uploaded_files)} files")
    return uploaded_files


def cleanup_local_files(**context):
    """
    Удаляет временные файлы
    """
    import shutil

    if os.path.exists(LOCAL_PATH):
        shutil.rmtree(LOCAL_PATH)
        logging.info(f"Cleaned up: {LOCAL_PATH}")


with DAG(
        dag_id='raw_walmart_from_kaggle_to_s3',
        default_args=default_args,
        description='Download Walmart dataset from Kaggle and upload to MinIO',
        schedule=None,  # Запускается вручную
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['walmart', 'kaggle', 'minio', 'ingestion'],

) as dag:
    task_download = PythonOperator(
        task_id='download_from_kaggle',
        python_callable=download_from_kaggle
    )

    task_upload = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio
    )

    task_cleanup = PythonOperator(
        task_id='cleanup_local_files',
        python_callable=cleanup_local_files
    )

    # Последовательность выполнения
    task_download >> task_upload >> task_cleanup