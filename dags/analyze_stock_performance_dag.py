from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock
import os
import psycopg2
import psycopg2.sql as sql
import psycopg2.extras as execute_values
from airflow.sdk.bases.hook import BaseHook



default_args = {
    'owner': 'Tuan Quang',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email': ['tbuiquang103@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

TICKERS = [
        'acb','bvh','bcm','bid','fpt','hdb','hpg','lpb',
        'msn','mbb','mwg','plx','gas','sab','stb','shb',
        'ssi','tcb','tpb','vcb','ctg','vjc','vib','gvr',
        'vnm','vre','vic','vhm','vpb'
    ]

def analyze_stock_performance(**kwargs):
    execution_date = kwargs['ds']
    year, month, _ = execution_date.split("-")

    airflow_conn = BaseHook.get_connection("local_postgres")

    conn = psycopg2.connect(
        host = airflow_conn.host,
        database = airflow_conn.schema,
        user = airflow_conn.login,
        password = airflow_conn.password
    )


    summary_data = [] 

    for ticker in TICKERS:
        table_name = f"vnstock_{ticker.lower()}"

        query = f"""
            SELECT Date, Close, Volume
            FROM {table_name}
            WHERE year = %s
            ORDER BY Date
        """

        df = pd.read_sql_query(
            query,
            conn,
            params=(year,)
        )

        df.columns = [c.lower() for c in df.columns]

        if df.empty or len(df) < 2:
            continue    
        start_price = df.iloc[0]['close']
        end_price = df.iloc[-1]['close']
        total_return = (end_price - start_price) / start_price 

        df['daily_return'] = df['close'].pct_change()
        volatility = df['daily_return'].std()

         # ---- KPI 3: Avg Volume ----
        avg_volume = df['volume'].mean()

        summary_data.append({
            'ticker': ticker.upper(),
            'year': 2025,
            'start_price': start_price,
            'end_price': end_price,
            'total_return': round(total_return, 4),
            'volatility': round(volatility, 4),
            'avg_volume': int(avg_volume)
        })

    conn.close()

    result_df = pd.DataFrame(summary_data)

    output_path = "/usr/local/airflow/dags/data_lake/vnstock_analysis"
    os.makedirs(output_path, exist_ok=True)

    output_file = os.path.join(output_path, "yearly_stock_summary_2025.csv")
    result_df.to_csv(output_file, index=False)

    print("=== YEARLY STOCK ANALYSIS  ===")
    print(result_df.sort_values("total_return", ascending=False))


def load_yearly_analysis_to_postgres(**kwargs):
    execution_date = kwargs['ds']
    year, _, _ = execution_date.split("-")

    airflow_conn = BaseHook.get_connection("local_postgres")

    conn = psycopg2.connect(
        host=airflow_conn.host,
        database=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password
    )
    cursor = conn.cursor()

    # 1. Tạo bảng nếu chưa tồn tại
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vnstock_yearly_analysis (
            ticker TEXT,
            year INT,
            start_price FLOAT,
            end_price FLOAT,
            total_return FLOAT,
            volatility FLOAT,
            avg_volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, year)
        );
    """)
    conn.commit()

    # 2. Đọc file CSV từ task analyze
    input_file = "/usr/local/airflow/dags/data_lake/vnstock_analysis/yearly_stock_summary_2025.csv"
    df = pd.read_csv(input_file)

    records = [
        (
            r['ticker'],
            year,
            r['start_price'],
            r['end_price'],
            r['total_return'],
            r['volatility'],
            r['avg_volume']
        )
        for _, r in df.iterrows()
    ]

    insert_query = """
        INSERT INTO vnstock_yearly_analysis
        (ticker, year, start_price, end_price, total_return, volatility, avg_volume)
        VALUES %s
        ON CONFLICT (ticker, year) DO UPDATE SET
            start_price = EXCLUDED.start_price,
            end_price = EXCLUDED.end_price,
            total_return = EXCLUDED.total_return,
            volatility = EXCLUDED.volatility,
            avg_volume = EXCLUDED.avg_volume,
            created_at = CURRENT_TIMESTAMP;
    """

    execute_values.execute_values(
        cursor,
        insert_query,
        records
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("Loaded yearly stock analysis into PostgreSQL")


with DAG(
    'analyze_stock_performance_dag',
    default_args=default_args,
    description='DAG phân tích hiệu suất cổ phiếu VNStock hàng năm',
    schedule='@yearly',  # Chạy vào ngày 1 tháng 1 hàng năm lúc 08:00 AM
    catchup=False,
    tags=['vnstock', 'analysis'],
) as dag:


    analyze_task = PythonOperator(
        task_id='analyze_stock_performance',
        python_callable=analyze_stock_performance,
    )

    load_analysis_task = PythonOperator(
        task_id='load_yearly_analysis_to_postgres',
        python_callable=load_yearly_analysis_to_postgres,
    )

    analyze_task >> load_analysis_task