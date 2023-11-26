import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
import sys
import datetime
import math

from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
    
spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.driver.cores", "2") \
                    .config("spark.driver.memory", "16g") \
                    .config("spark.dynamicAllocation.enabled", "true") \
                    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
                    .getOrCreate()

date = sys.argv[1]
depth = sys.argv[2]
events_path = sys.argv[3]
cities_path = sys.argv[4]
target_path = sys.argv[5]


#Пишем функцию get_distance, которая вычисляет расстояние между двумя географическими точками, используя формулу Хаверсина.
def get_distance(lat_1, lat_2, lng_1, lng_2):
    lat_1=(math.pi / 180) * lat_1
    lat_2=(math.pi / 180) * lat_2
    lng_1=(math.pi / 180) * lng_1
    lng_2=(math.pi / 180) * lng_2
 
    return  2 * 6371 * math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1) / 2), 2) +
    math.cos(lat_1) * math.cos(lat_2) * math.pow(math.sin((lng_2 - lng_1) / 2),2)))

udf_func=F.udf(get_distance)


def input_paths(date, depth, events_path):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")

    return [
        f"{events_path}/date={(dt-datetime.timedelta(days=day)).strftime('%Y-%m-%d')}"
        for day in range(int(depth))
    ]

cities = spark.read.csv("/user/staceykuzm/geo.csv", sep = ";", header = True) \
        .withColumn("lat", F.col("lat").cast(DoubleType())) \
        .withColumn("lng", F.col("lng").cast(DoubleType())) \
        .withColumnRenamed("lat", "lat_c")
        .withColumnRenamed("lng", "lng_c")

messages_cities = (
    events_messages.crossJoin(cities)
    .withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lng"), F.col("lng_c")).cast(
            "float"
        ),
    )
    .withColumn(
        "distance_rank",
        F.row_number().over(
            Window().partitionBy(["user_id", "message_id"]).orderBy("distance")
        ),
    )
    .where("distance_rank == 1")
    .select ("user_id", "messages_id", "date", "datetime", "city", "timezone")
)

active_messages_cities = (
    events_messages.withColumn(
        "datetime_rank",
        F.row_number().over(
            Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
        ),
    )
    .where("datetime_rank == 1")
    .orderBy("user_id")
    .crossJoin(cities)
    .withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lng"), F.col("lng_c")).cast(
            "float"
        ),
    )
    .withColumn(
        "distance_rank",
        F.row_number().over(
            Window().partitionBy(["user_id"]).orderBy(F.asc("distance"))
        ),
    )
    .where("distance_rank == 1")
    .select("user_id", F.col("city").alias("act_city"), "date", "timezone")
) 

def main():

    spark = (
        SparkSession.builder.master("local")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # вычисляем датасет со всеми сообщениями
    events_messages = (
        events.where(F.col("event_type") == "message")
        .withColumn(
            "date",
            F.date_trunc(
                "day",
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
            ),
        )
        .selectExpr(
            "event.message_from as user_id",
            "event.message_id",
            "date",
            "event.datetime",
            "lat",
            "lon",
        )
    )

    # рассчитываем таблицу с изменениями города отправки сообщения
    temp_df = (
        messages_cities.withColumn(
            "max_date", F.max("date").over(Window().partitionBy("user_id"))
        )
        .withColumn(
            "city_lag",
            F.lead("act_city", 1, "empty").over(
                Window().partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .filter(F.col("act_city") != F.col("city_lag"))
    )

    # рассчитываем адрес города, из которого были отправлены 27 дней подряд сообщения от пользователя
    home_city = (
        temp_df.withColumnRenamed("city", "home_city")
        .withColumn(
            "date_lag",
            F.coalesce(
                F.lag("date").over(
                    Window().partitionBy("user_id").orderBy(F.col("date").desc())
                ),
                F.col("max_date"),
            ),
        )
        .withColumn("date_diff", F.datediff(F.col("date_lag"), F.col("date")))
        .where(F.col("date_diff") > 27)
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("date").desc())
            ),
        )
        .where(F.col("rank") == 1)
        .drop("date_diff", "date_lag", "max_date", "city_lag", "rank")
    )

    # рассчитываем кол-во смен города по каждому пользователю и список городов, которые посетил пользователь
    travel_list = temp_df.groupBy("user_id").agg(
        F.count("*").alias("travel_count"),
        F.collect_list("city").alias("travel_array")
    )

    # рассчитываем локальное время
    time_local = active_messages_cities.withColumn(
        "localtime", F.from_utc_timestamp(F.col("date"), F.col("timezone"))
    ).drop("timezone", "city", "date", "datetime", "act_city")

    # объединяем все данные в одну витрину
    final = (
        active_messages_cities.select("user_id", "act_city")
        .join(home_city, "user_id", "left")
        .join(travel_list, "user_id", "left")
        .join(time_local, "user_id", "left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "localtime",
        )
    )

    # записываем результат по заданному пути
    final.write.mode("overwrite").parquet(
        f"{target_path}/mart/users/_{date}_{depth}"
    )


if __name__ == "__main__":

    main()