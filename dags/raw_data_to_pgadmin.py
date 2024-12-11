import logging
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PGADMIN4_CONN = 'pg_db'
NEON_CONN = 'neon_db'
ALL_TABLE_FROM_NEON_DB = [
    'retail_raw_data_1', 'retail_raw_data_2', 'retail_raw_data_3', 'retail_raw_data_4', 
    'retail_raw_data_5', 'retail_raw_data_6', 'retail_raw_data_7',
]

ALL_TABLE_FROM_PGADMIN = ['supermarket', 'goods', 'category', 'location']

def _create_pgadmin_table():
    """
    Create the PGADMIN table with a desired schema
    """

    create_location_table = """
        CREATE TABLE IF NOT EXISTS location (
            id SERIAL NOT NULL,
            postal_code TEXT NOT NULL,
            city TEXT,
            state TEXT,
            region TEXT,
            country TEXT,
            PRIMARY KEY(postal_code)
        )
    """

    create_category_table = """
        CREATE TABLE IF NOT EXISTS category (
            id SERIAL PRIMARY KEY NOT NULL,
            name TEXT
        )
    """

    create_goods_table = """
        CREATE TABLE IF NOT EXISTS goods (
            id SERIAL PRIMARY KEY NOT NULL,
            category_id INT,
            supermarket_code TEXT,
            name TEXT,
            sales FLOAT,
            quantity INT,
            discount FLOAT,
            profit FLOAT,
            FOREIGN KEY (category_id) REFERENCES category(id),
            FOREIGN KEY (supermarket_code) REFERENCES location(postal_code)
        )
    """

    create_supermarket_table = """
        CREATE TABLE IF NOT EXISTS supermarket (
            id SERIAL PRIMARY KEY NOT NULL,
            location_code TEXT,
            segment TEXT,
            ship_mode TEXT,
            FOREIGN KEY (location_code) REFERENCES location(postal_code)
        )
    """

    create_table = [
        create_location_table,
        create_category_table,
        create_goods_table,
        create_supermarket_table
    ]

    postgres_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    for query in create_table:
        try:
            cursor.execute(query)
            conn.commit()
            logging.info("4 tables created successfully in pgadmin server")
        except Exception as e:
            conn.rollback()
            logging.error(f'Tables cannot be created due to: {e}')

def _get_category():
    neon_hook = PostgresHook(postgres_conn_id=NEON_CONN)
    conn = neon_hook.get_conn()
    cursor = conn.cursor()

    category_data = []
    for table_name in ALL_TABLE_FROM_NEON_DB:
        sql = f"SELECT category FROM {table_name}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for data in rows:
            if data not in category_data:
                category_data.append(data)
        
        logging.info(f"Get category form {table_name} successfully")

    return category_data

def _get_sub_category():
    neon_hook = PostgresHook(postgres_conn_id=NEON_CONN)
    conn = neon_hook.get_conn()
    cursor = conn.cursor()

    sub_category_data = []

    for table_name in ALL_TABLE_FROM_NEON_DB:
        sql = f"SELECT category, postal_code, sub_category, sales, quantity, discount, profit FROM {table_name}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for data in rows:
            sub_category_data.append(data)
        
        logging.info(f"Get sub category form {table_name} successfully")

    return sub_category_data

def _get_location():
    neon_hook = PostgresHook(postgres_conn_id=NEON_CONN)
    conn = neon_hook.get_conn()
    cursor = conn.cursor()

    location = []
    postal_code_arr = []

    for table_name in ALL_TABLE_FROM_NEON_DB:
        sql = f"SELECT postal_code, city, state, region, country FROM {table_name}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for data in rows:
            if data[0] not in postal_code_arr:
                postal_code_arr.append(data[0])
                location.append(data)               

        logging.info(f"{table_name} get {len(location)} items")

    logging.info(f"GET LOCATION DATA FROM ALL TABLE COMPLETE")
    return location

def _get_supermarket():
    neon_hook = PostgresHook(postgres_conn_id=NEON_CONN)
    conn = neon_hook.get_conn()
    cursor = conn.cursor()

    supermarket_arr = []
    postal_code_arr = []

    for table_name in ALL_TABLE_FROM_NEON_DB:
        sql = f"SELECT postal_code, segment, ship_mode FROM {table_name}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for data in rows:
            if data[0] not in postal_code_arr:
                postal_code_arr.append(data[0])
                supermarket_arr.append(data)              

        logging.info(f"{table_name} get {len(supermarket_arr)} items")

    logging.info(f"GET SUPERMARKET DATA FROM ALL TABLE COMPLETE")
    return supermarket_arr

def _clear_all_data():
    # Connecting database
    pg_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for table in ALL_TABLE_FROM_PGADMIN:
        try:
            # Delete existing data from table
            cursor.execute(f"DELETE FROM {table}")
            conn.commit()
            logging.info(f"Existing data deleted from {table} table.")

            # Reset the sequence for the id column
            cursor.execute(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1")
            conn.commit()
            logging.info(f"{table} table: ID sequence reset to start with 1.")
        except Exception as e:
            conn.rollback()
            logging.error("Failed to delete existing data: %s", e)
            cursor.close()
            conn.close()
            raise

def _insert_category(**context):
    # SQL queries
    insert_data_query = "INSERT INTO category (name) VALUES (%s)"

    # Connecting database
    pg_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    ti = context["ti"]
    all_data = ti.xcom_pull(task_ids="get_category_data", key="return_value")

    for data in all_data:
        try:
            cursor.execute(insert_data_query, data)
            conn.commit()
            logging.info(f"INSERT DATA INTO catagory TABLE COMPLETE")
        except Exception as e:
            conn.rollback()
            logging.error("Failed to insert data Error: %s", e)
    
    # cursor.close()
    # conn.close()

def _insert_goods(**context):
    insert_data_query = """
        INSERT INTO goods (category_id, supermarket_code, name, sales, quantity, discount, profit) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    # Connecting database
    pg_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    ti = context["ti"]
    all_data = ti.xcom_pull(task_ids="get_sub_category_data", key="return_value")
    row_count = 0
    for data in all_data:
        sql = f"SELECT id FROM category WHERE name='{data[0]}'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        categoryId = str(rows[0]).strip("(),")
        try:
            cursor.execute(insert_data_query, (
                int(categoryId), data[1], data[2], float(data[3]), int(data[4]), float(data[5]), float(data[6])
            ))
            conn.commit()
            row_count += 1
        except Exception as e:
            conn.rollback()
            logging.error("Failed to insert data Error: %s", e)
        
    logging.info(f"{row_count} rows inserted into table category")

def _insert_location(**context):
    insert_data_query = """
        INSERT INTO location (postal_code, city, state, region, country) 
        VALUES (%s, %s, %s, %s, %s)
    """
    # Connecting database
    pg_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    ti = context["ti"]
    all_data = ti.xcom_pull(task_ids="get_location_data", key="return_value")
    row_count = 0
    for data in all_data:
        try:
            cursor.execute(insert_data_query, data)
            conn.commit()
            row_count += 1
        except Exception as e:
            conn.rollback()
            logging.error("Failed to insert data Error: %s", e)
    
    logging.info(f"{row_count} rows inserted into table category")

def _insert_supermarket(**context):
    insert_data_query = """
        INSERT INTO supermarket (location_code, segment, ship_mode) 
        VALUES (%s, %s, %s)
    """
    # Connecting database
    pg_hook = PostgresHook(postgres_conn_id=PGADMIN4_CONN)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    ti = context["ti"]
    all_data = ti.xcom_pull(task_ids="get_supermarket_data", key="return_value")
    row_count = 0
    for data in all_data:
        try:
            cursor.execute(insert_data_query, data)
            conn.commit()
            row_count += 1
        except Exception as e:
            conn.rollback()
            logging.error("Failed to insert data Error: %s", e)
    
    logging.info(f"{row_count} rows inserted into table supermarket")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG('raw_data_to_pgadmin', default_args=default_args, schedule_interval='@daily', catchup=False):
    create_pgadmin_table = PythonOperator(
        task_id = 'create_pgadmin_table',
        python_callable=_create_pgadmin_table
    )

    get_category = PythonOperator(
        task_id = 'get_category_data',
        python_callable=_get_category
    )

    get_sub_category = PythonOperator(
        task_id = 'get_sub_category_data',
        python_callable = _get_sub_category
    )

    get_location = PythonOperator(
        task_id = 'get_location_data',
        python_callable = _get_location
    )

    get_supermarket = PythonOperator(
        task_id = 'get_supermarket_data',
        python_callable = _get_supermarket
    )

    clear_all_data = PythonOperator(
        task_id = 'clear_data',
        python_callable = _clear_all_data
    )

    insert_category = PythonOperator(
        task_id = 'insert_category_to_table',
        python_callable = _insert_category
    )

    insert_goods = PythonOperator(
        task_id = 'insert_goods_to_table',
        python_callable = _insert_goods
    )

    insert_location = PythonOperator(
        task_id = 'insert_location_to_table',
        python_callable = _insert_location
    )

    insert_supermarket = PythonOperator(
        task_id = 'insert_supermarket_to_table',
        python_callable = _insert_supermarket
    )

    (
        create_pgadmin_table >> 
        [get_category, get_sub_category, get_location, get_supermarket] >> 
        clear_all_data >> 
        [insert_category, insert_location] >> insert_goods >> insert_supermarket
    )