from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import requests
import logging
import json
from utils.functions import obtener_datos_api_prod, insertar_datos_en_postgres_prod, obtener_datos_api_carts, insertar_datos_en_postgres_carts

args = {
    'owner':"Santiago Colantonio",
    'start_date':days_ago(1)
}

dag = DAG(
    dag_id='my_dag',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
 # Tarea 1: Obtener datos de la API
    tarea_obtener_datos_prods = PythonOperator(
        task_id='obtener_datos_api_prods',
        python_callable=obtener_datos_api_prod,
        provide_context=True,
        retries=1,
    )

    # Tarea 2: Insertar datos en PostgreSQL
    tarea_insertar_datos_prods = PythonOperator(
        task_id='insertar_datos_en_postgres',
        python_callable=insertar_datos_en_postgres_prod,
        provide_context=True,
        retries=1,
    )

        # Tarea 1: Obtener datos de la API
    tarea_obtener_datos_carts = PythonOperator(
        task_id='obtener_datos_api_carts',
        python_callable=obtener_datos_api_carts,
        provide_context=True,
        retries=1,
    )

    # Tarea 2: Insertar datos en PostgreSQL
    tarea_insertar_datos_carts = PythonOperator(
        task_id='insertar_datos_carts_en_postgres',
        python_callable=insertar_datos_en_postgres_carts,
        provide_context=True,
        retries=1,
    )


    # Definir dependencias
    tarea_obtener_datos_prods >> tarea_insertar_datos_prods

    tarea_insertar_datos_prods >> tarea_obtener_datos_carts

    tarea_obtener_datos_carts >> tarea_insertar_datos_carts


