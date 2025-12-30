from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = "prnproject477"
REGION = "asia-southeast1"
CLUSTER_NAME = "mycluster"
PYSPARK_FILE = "gs://prn-spark-bucket/scripts/spark_example.py"


@dag(
    dag_id="spark_job_to_dataproc_astro",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["dataproc", "spark"],
)
def spark_job_flow():

    @task
    def build_spark_job():
        return {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_FILE},
        }

    job_config = build_spark_job()

    run_spark = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=job_config,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_conn",
    )

    job_config >> run_spark


spark_job_flow()
