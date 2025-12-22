from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock
import os
import psycopg2
import psycopg2.sql as sql
import psycopg2.extras as execute_values
import airflow.hooks.base as base_hook
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'Tuan Quang',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email': ['tuanquang@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

BASE_DIR = "/usr/local/airflow/dags/data_lake/vnstock_prices_csv"

airflow_conn = base_hook.BaseHook.get_connection("local_postgres")

def Extract_data_to_csv (**kwargs):
    execution_date = kwargs["ds"]
    year, month, _ = execution_date.split("-")

    list_csv_files = []
    vnstock_api = Vnstock()

    TICKERS = ['ACB', 'BVH', 'BCM', 'BID', 'FPT', 'HDB', 'HPG', 'LPB',
 'MSN', 'MBB', 'MWG', 'PLX', 'GAS', 'SAB', 'STB', 'SHB',
 'SSI', 'TCB', 'TPB', 'VCB', 'CTG', 'VJC', 'VIB', 'GVR',
 'VNM', 'VRE', 'VIC', 'VHM', 'VPB']


    for ticker in TICKERS:
        try:
            stock_obj = vnstock_api.stock(symbol=ticker, source="VCI")
            df = stock_obj.quote.history(
                start=f"{year}-01-01",
                end=execution_date,
                interval="1D"
            )

            if not df.empty:
                df.reset_index(inplace=True)
                df['Ticker'] = ticker
                df['year'] = pd.to_datetime(df['Date']).dt.year
                df['month'] = pd.to_datetime(df['Date']).dt.month

                output_dir = os.path.join(BASE_DIR, f"year={year}")
                os.makedirs(output_dir, exist_ok=True)
                output_file = os.path.join(output_dir, f"{ticker}_execution_date_{execution_date}.csv")
                df.to_csv(output_file, index=False)
                list_csv_files.append(output_file)
            else:
                print(f"No data for {ticker} on {execution_date}")

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    return list_csv_files

def create_table_if_not_exists( **kwargs):
    conn = psycopg2.connect(
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port
    )
    ti = kwargs['ti']
    list_files = ti.xcom_pull(task_ids='extract_data_to_csv')
    cursor = conn.cursor()
    for file_name in list_files:
        if file_name.endswith(".csv"):
            table_name = "vnstock_prices"
            break
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date DATE,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT,
            Ticker VARCHAR(10),
            year INT,
            month INT,
            PRIMARY KEY (Date, Ticker)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
    cursor.close()
    conn.close()

def load_csv_to_postgres(**kwargs):
    conn = psycopg2.connect(
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port
    )    
    cursor = conn.cursor()
    execution_date = kwargs["ds"]
    year, month, _ = execution_date.split("-")

    ti = kwargs['ti']
    list_files = ti.xcom_pull(task_ids='extract_data_to_csv')

    data_dir = os.path.join(BASE_DIR, f"year={year}")   
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".csv") and file_name in list_files:
            file_path = os.path.join(data_dir, file_name)
            df = pd.read_csv(file_path)

            records = df.to_dict(orient='records')

            insert_query = sql.SQL(f"""
                INSERT INTO {file_name} (Date, Open, High, Low, Close, Volume, Ticker, year, month)
                VALUES %s
                ON CONFLICT (Date, Ticker) DO NOTHING
            """)

            values = [
                (
                    record['Date'],
                    record['Open'],
                    record['High'],
                    record['Low'],
                    record['Close'],
                    record['Volume'],
                    record['Ticker'],
                    record['year'],
                    record['month']
                ) for record in records
            ]

            execute_values.execute_values(
                cursor, insert_query.as_string(conn), values
            )
            conn.commit()

with DAG(
    'vnstock_etl_dag',
    default_args=default_args,
    description='ETL DAG for Vnstock data to Postgres',
    schedule='@yearly',
    catchup=True,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_to_csv',
        python_callable=Extract_data_to_csv,
    )

    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists,
    )

    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    extract_task >> create_table_task >> load_task
