from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import logging
import json

def hello():
    print('Hola ya estoy probando de una manera externa!')


# Función para obtener datos de la API
def obtener_datos_api_prod(**kwargs):
    API_URL = 'https://fakestoreapi.com/products'
    try:
        logging.info(f"Haciendo solicitud a la API: {API_URL}")
        response = requests.get(API_URL)
        response.raise_for_status()  # Verifica que la respuesta sea 200
        data = response.json()
        logging.info(f"Datos obtenidos de la API: {data[0]}")  # Log para mostrar los datos
        kwargs['ti'].xcom_push(key='api_data_prods', value=data)  # Guardar datos en XCom
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al hacer solicitud a la API: {e}")
        raise

# Función para insertar datos en PostgreSQL
def insertar_datos_en_postgres_prod(**kwargs):
    # Obtener los datos de XCom
    data = kwargs['ti'].xcom_pull(key='api_data_prods', task_ids='obtener_datos_api_prods')

    logging.info(f"Datos obtenidos de XCom: {data}")  # Log para verificar los datos obtenidos de XCom

    # Validar que los datos existan
    if not data:
        logging.error("No se encontraron datos en XCom para insertar.")
        raise ValueError("No se encontraron datos para insertar en la base de datos.")

    try:
        # Establecer la conexión con PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Consulta de inserción
        insert_query = """
        INSERT INTO products (id, title, price, description, category)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;  -- Evitar duplicados
        """
        
        # Insertar todos los productos
        for product in data:
            cursor.execute(insert_query, (product['id'], product['title'], product['price'], product['description'], product['category']))
        
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Datos insertados exitosamente en PostgreSQL.")
    
    except Exception as e:
        logging.error(f"Error al insertar los datos en la base de datos: {e}")
        raise

# Función para obtener datos de la API
def obtener_datos_api_carts(**kwargs):
    API_URL = 'https://fakestoreapi.com/carts'
    try:
        logging.info(f"Haciendo solicitud a la API: {API_URL}")
        response = requests.get(API_URL)
        response.raise_for_status()  # Verifica que la respuesta sea 200
        data = response.json()
        logging.info(f"Datos obtenidos de la API: {data[0]}")  # Log para mostrar los datos
        kwargs['ti'].xcom_push(key='api_data_carts', value=data)  # Guardar datos en XCom
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al hacer solicitud a la API: {e}")
        raise

def insertar_datos_en_postgres_carts(**kwargs):
    # Obtener los datos de XCom
    data = kwargs['ti'].xcom_pull(key='api_data_carts', task_ids='obtener_datos_api_carts')
    
    logging.info(f"Datos obtenidos de XCom: {data}")  # Log para verificar los datos obtenidos de XCom

    # Validar que los datos existan
    if not data:
        logging.error("No se encontraron datos en XCom para insertar.")
        raise ValueError("No se encontraron datos para insertar en la base de datos.")

    try:
        # Establecer la conexión con PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Consulta de inserción
        insert_query_cart = """
        INSERT INTO carts (id, user_id, date)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING;  -- Evitar duplicados
        """
        insert_query_prod_x_cart = """
        INSERT INTO prod_x_cart (id, product_id, quantity)
        VALUES (%s, %s, %s)
        ON CONFLICT (id, product_id) DO NOTHING;  -- Evitar duplicados
        """

        # Insertar todos los productos
        cart_insertions = []
        prod_x_cart_insertions = []

        for cart in data:
            cart_insertions.append((cart['id'], cart['userId'], cart['date']))
            products = cart['products']
            for product in products:
                prod_x_cart_insertions.append((cart['id'], product['productId'], product['quantity']))
        
        logging.info(f"Trying to insert in carts: \n {cart_insertions}")
        logging.info(f"Trying to insert in prod_x_cart: \n {prod_x_cart_insertions}")
        
        # Ejecutar inserciones masivas
        if cart_insertions:
            cursor.executemany(insert_query_cart, cart_insertions)

        if prod_x_cart_insertions:
            cursor.executemany(insert_query_prod_x_cart, prod_x_cart_insertions)

        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Datos insertados exitosamente en PostgreSQL.")
    
    except Exception as e:
        logging.e
