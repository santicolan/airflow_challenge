# Airflow Challenge

Este proyecto es un DAG de Airflow diseñado para extraer datos de una API y cargarlos en una base de datos PostgreSQL.

## Descripción del Proyecto

El DAG realiza las siguientes tareas:
- Obtener datos de la API de productos.
- Insertar los datos obtenidos en PostgreSQL.
- Obtener datos de la API de carritos de compras.
- Insertar los datos obtenidos en PostgreSQL.

## Requisitos

- Airflow 2.x
- PostgreSQL

## Uso

Para ejecutar este DAG, asegúrate de tener configurado Airflow y PostgreSQL en tu entorno. Luego, simplemente activa el DAG en la interfaz de Airflow y ejecuta manualmente o deja que siga su schedule diario.

## Estructura del Proyecto

- `dags/`: contiene el DAG principal (`my_dag.py`).
- `utils/`: contiene las funciones auxiliares utilizadas por el DAG.
