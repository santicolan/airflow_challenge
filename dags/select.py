from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

# Función para mostrar los resultados
def mostrar_resultados(**kwargs):
    # Obtener los resultados de XCom
    ti = kwargs['ti']
    resultados = ti.xcom_pull(task_ids='hacer_select')
    
    # Mostrar resultados
    for fila in resultados:
        print(fila)

# Argumentos del DAG
default_args = {
    'owner': 'Santiago Colantonio',
    'start_date': days_ago(1),
}

# Definición del DAG
with DAG(
    dag_id='select_dag',
    default_args=default_args,
    schedule_interval=None,  # Cambiar a None para ejecutar manualmente
    catchup=False,
) as dag:

    # Tarea para hacer el SELECT
    hacer_select = PostgresOperator(
        task_id='hacer_select',
        postgres_conn_id='my_postgres',  # Reemplaza con tu ID de conexión
        sql="""
            SELECT * FROM vw_aggregated;
        """,
        # sql="""
        #     TRUNCATE TABLE prod_x_cart;
        # """,
        do_xcom_push=True,  # Para que los resultados se envíen a XCom
    )

    # Tarea para mostrar los resultados
    mostrar_resultados_task = PythonOperator(
        task_id='mostrar_resultados',
        python_callable=mostrar_resultados,
        provide_context=True,
    )

    # Definir dependencias
    hacer_select >> mostrar_resultados_task
