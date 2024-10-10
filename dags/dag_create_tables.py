from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import requests
import logging
import json
from utils.functions import hello

args = {
    'owner':"Santiago Colantonio",
    'start_date':days_ago(1)
}

dag = DAG(
    dag_id='dag_create_tables',
    default_args=args,
)

with dag:
    hello_world = PythonOperator(
        task_id = 'hello',
        python_callable = hello
    )

    tarea_crear_prods = PostgresOperator(
        task_id = 'crear_tabla_prods',
        postgres_conn_id='my_postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            price DECIMAL(10,2),
            description TEXT,
            category TEXT
        );
        """
    )

    tarea_crear_tabla_carts = PostgresOperator(
        task_id='crear_tabla_carts',
        postgres_conn_id='my_postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS carts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            date DATE
        );
        """
    )

    crear_tabla_prod_x_cart = PostgresOperator(
        task_id='crear_tabla_prod_x_cart',
        postgres_conn_id='my_postgres',
        sql = """
        DO $$ 
        BEGIN
            -- Verificar si la tabla existe
            IF NOT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'prod_x_cart'
            ) THEN
                -- Si la tabla no existe, crearla
                CREATE TABLE prod_x_cart (
                    id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    PRIMARY KEY (id, product_id)
                );

                -- Agregar restricciones de claves foráneas
                ALTER TABLE prod_x_cart ADD CONSTRAINT fk_prod_x_cart_cart_id
                FOREIGN KEY (id) REFERENCES carts(id);
                
                ALTER TABLE prod_x_cart ADD CONSTRAINT fk_prod_x_cart_prod_id
                FOREIGN KEY (product_id) REFERENCES products(id);
            END IF;
        END $$;

                """
    )

    #  Tarea para generar la tabla agregada
    tarea_generar_tabla_agregada = PostgresOperator(
        task_id='generar_tabla_agregada',
        postgres_conn_id='my_postgres',
        sql="""
        CREATE OR REPLACE VIEW vw_aggregated AS
        (
        SELECT p.title, c.date, SUM(p.price*COALESCE(pxc.quantity,0)) revenue
        FROM products p 
        INNER JOIN prod_x_cart pxc ON p.id = pxc.product_id
        INNER JOIN carts c ON c.id = pxc.id
        GROUP BY  p.title, c.date
        ORDER BY 1,2)
        """
    )


