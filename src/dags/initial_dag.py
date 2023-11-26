from datetime import datetime
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
}
    
dag_spark = DAG(
    dag_id="initial_dag",
    default_args=default_args,
    schedule_interval=None,
)
    
initial_load = SparkSubmitOperator(
    task_id="initial_load",
    dag=dag_spark,
    application="/lessons/initial_load.py",
    conn_id="yarn_spark",
    application_args=[
        "/user/master/data/geo/events/",
        "/user/staceykuzm/data/geo/events/",
    ],
    conf={
        "spark.driver.masResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = "2g"
)

initial_load