"""
DAG: Load data from Minio (s3) to PostgreSQL
Load CSV files from Minio bucket to PostgreSQL raw schema
"""

import logging
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO


# Конфигурация DAG
SCHEMA = "raw"
MINIO_BUCKET = "walmart-raw"
# Подключения
MINIO_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "postgres_conn"
DAG_ID='s3_to_postgres_walmart'


# Порядок загрузки (учитываем FK)
TABLES_CONFIG = [
    # Сначала справочники (нет FK зависимостей)
    {"s3_key": "raw/customers.csv", "table": "customers", "rename_cols": None},
    {"s3_key": "raw/sellers.csv", "table": "sellers", "rename_cols": None},
    {"s3_key": "raw/products.csv", "table": "products", "rename_cols": {"product category": "product_category"}},
    {"s3_key": "raw/geolocation.csv", "table": "geolocation", "rename_cols": None},
    # Затем orders (зависит от customers)
    {"s3_key": "raw/orders.csv", "table": "orders", "rename_cols": None},
    # В конце таблицы с FK на orders
    {"s3_key": "raw/order_items.csv", "table": "order_items", "rename_cols": None},
    {"s3_key": "raw/payments.csv", "table": "payments", "rename_cols": None},
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_table_to_postgres(s3_key: str, table: str, rename_cols: dict = None, **context):
    """
    Загружает CSV из MinIO в PostgreSQL
    """
    from sqlalchemy import text

    logging.info(f"Loading {s3_key} to {SCHEMA}.{table}")

    # Подключение к MinIO
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # Скачиваем файл
    s3_object = s3_hook.get_key(key=s3_key, bucket_name=MINIO_BUCKET)
    file_content = s3_object.get()['Body'].read()

    # Читаем в DataFrame
    df = pd.read_csv(BytesIO(file_content))
    logging.info(f"Read {len(df)} rows from {s3_key}")

    # Переименовываем колонки если нужно
    if rename_cols:
        df = df.rename(columns=rename_cols)
        logging.info(f"Renamed columns: {rename_cols}")

    # Подключение к PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Очищаем таблицу перед загрузкой (TRUNCATE с CASCADE для FK)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table} CASCADE"))

    # Загружаем данные
    df.to_sql(
        name=table,
        con=engine,
        schema=SCHEMA,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=10000
    )

    logging.info(f"Successfully loaded {len(df)} rows to {SCHEMA}.{table}")
    return len(df)


def create_load_task(dag, config, upstream_task=None):
    """
    Создаёт task для загрузки таблицы
    """
    task = PythonOperator(
        task_id=f"load_{config['table']}",
        python_callable=load_table_to_postgres,
        op_kwargs={
            's3_key': config['s3_key'],
            'table': config['table'],
            'rename_cols': config['rename_cols']
        },
        dag=dag
    )

    if upstream_task:
        upstream_task >> task

    return task


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description='Load Walmart data from MinIO to PostgreSQL',
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['walmart', 'postgres', 'etl'],
) as dag:
    # Создаём tasks
    # Группа 1: независимые таблицы (параллельно)
    task_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[0]
    )

    task_sellers = PythonOperator(
        task_id='load_sellers',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[1]
    )
    task_products = PythonOperator(
        task_id='load_products',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[2]
    )

    task_geolocation = PythonOperator(
        task_id='load_geolocation',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[3]
    )

    # Группа 2: orders (после customers)
    task_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[4]
    )

    # Группа 3: зависят от orders (параллельно)
    task_order_items = PythonOperator(
        task_id='load_order_items',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[5]
    )
    task_payments = PythonOperator(
        task_id='load_payments',
        python_callable=load_table_to_postgres,
        op_kwargs=TABLES_CONFIG[6]
    )

    # Определяем зависимости
    # Справочники → orders → order_items/payments
    [task_customers, task_sellers, task_products, task_geolocation] >> task_orders
    task_orders >> [task_order_items, task_payments]