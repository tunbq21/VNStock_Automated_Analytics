from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import requests
import psycopg2
from psycopg2 import sql
from airflow.hooks.base import BaseHook


# ------------------- CẤU HÌNH DAG -------------------
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

TABLE_NAME = 'shopee_ratings_etl'

dag = DAG(
    dag_id='crawl_shopee_ratings',
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule='@daily',
    catchup=False,
    description='DAG crawl Shopee ratings and save to PostgreSQL',
    tags=['shopee', 'ratings']
)


# ------------------- HÀM XỬ LÝ DỮ LIỆU -------------------
def crawl_and_save():
    url = "https://shopee.vn/api/v2/item/get_ratings?flag=1&limit=10&request_source=3&exclude_filter=1&fold_filter=0&relevant_reviews=false&itemid=29911154536&shopid=487028617&filter=0&inherit_only_view=false&fe_toggle=%5B2%2C3%5D&preferred_item_item_id=29911154536&preferred_item_shop_id=487028617&preferred_item_include_type=1&offset=0"
    
    headers = {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "vi-VN,vi;q=0.9",
        "content-type": "application/json",
        "referer": "https://shopee.vn/",
        "user-agent": "Mozilla/5.0",
        "x-api-source": "rweb"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {response.status_code}")

    data = response.json()
    ratings = data.get("data", {}).get("ratings", [])

    if not ratings:
        print("No ratings found.")
        return

    conn = None
    try:
        airflow_conn = BaseHook.get_connection("local_postgres")

        conn = psycopg2.connect(
            host=airflow_conn.host,
            port=airflow_conn.port,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password
        )

        cur = conn.cursor()

        # Tạo bảng nếu chưa có
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                orderid BIGINT,
                itemid BIGINT,
                cmtid BIGINT,
                ctime TIMESTAMP,
                rating INTEGER,
                rating_star INTEGER,
                userid BIGINT,
                shopid BIGINT,
                comment TEXT,
                status INTEGER,
                mtime TIMESTAMP
            )
        """).format(sql.Identifier(TABLE_NAME))

        cur.execute(create_table_query)
        conn.commit()

        # Query insert
        insert_query = sql.SQL("""
            INSERT INTO {} (
                orderid, itemid, cmtid, ctime, rating,
                rating_star, userid, shopid, comment,
                status, mtime
            )
            VALUES (%s, %s, %s, to_timestamp(%s), %s,
                    %s, %s, %s, %s, %s, to_timestamp(%s))
        """).format(sql.Identifier(TABLE_NAME))

        for r in ratings:
            cur.execute(insert_query, (
                r.get("orderid", 0),
                r.get("itemid"),
                r.get("cmtid"),
                r.get("ctime"),
                r.get("rating"),
                r.get("rating_star"),
                r.get("userid"),
                r.get("shopid"),
                r.get("comment", ""),
                r.get("status"),
                r.get("mtime")
            ))

        conn.commit()
        cur.close()

    finally:
        if conn:
            conn.close()


# ------------------- TASK -------------------
crawl_task = PythonOperator(
    task_id='crawl_ratings',
    python_callable=crawl_and_save,
    dag=dag
)

crawl_task
