from datetime import datetime
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
    dag_id="sprint_7_dag",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)
    
users_mart = SparkSubmitOperator(
    task_id="users_mart",
    dag=dag_spark,
    application="/lessons/users_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/staceykuzm/data/geo/events/",
        "/user/staceykuzm/geo.csv",
        "/user/staceykuzm/data/analytics/",
    ],
    conf={
        "spark.driver.masResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = "2g"
)
    
geo_mart = SparkSubmitOperator(
    task_id="geo_mart",
    dag=dag_spark,
    application="/lessons/geo_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/staceykuzm/data/geo/events/",
        "/user/staceykuzm/geo.csv",
        "/user/staceykuzm/data/analytics/",
    ],
    conf={
        "spark.driver.masResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = "2g"
)
    
recommendations_mart = SparkSubmitOperator(
    task_id="recommendations_mart",
    dag=dag_spark,
    application="/lessons/recommendations_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/staceykuzm/data/geo/events/",
        "/user/staceykuzm/geo.csv",
        "/user/staceykuzm/data/analytics/",
    ],
    conf={
        "spark.driver.masResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = "2g"
)

users_mart >> geo_mart >> recommendations_mart
