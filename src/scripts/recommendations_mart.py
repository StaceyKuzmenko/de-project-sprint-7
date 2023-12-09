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

from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, expr, year, month, dayofmonth
    
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
        .withColumnRenamed("lat", "lat_c") \
        .withColumnRenamed("lng", "lng_c")

def main():

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # вычисляем датасет со всеми подписками
    events_subscriptions = (
        events.filter(F.col("event_type") == "subscription")
        .where(
            F.col("event.subscription_channel").isNotNull()
            & F.col("event.user").isNotNull()
        )
        .select(
            F.col("event.subscription_channel").alias("channel_id"),
            F.col("event.user").alias("user_id")
        )
        .distinct()
    )

    #ищем пары пользователей, которые подписаны на один и тот же канал
    cols = ["user_left", "user_right"]
    subscriptions = (
        events_subscriptions.withColumnRenamed("user_id", "user_left")
        .join(
            events_subscriptions.withColumnRenamed("user_id", "user_right"),
            on="channel_id",
            how="inner",
        )
        .drop("channel_id")
        .filter(F.col("user_left") != F.col("user_right"))
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
    )

    #пишем функцию, которая будет определять отправителей и получателей сообщений
    def finding_senders_and_recipients (message_from, message_to):
        return (
            events.filter("event_type == 'message'")
            .where(
                F.col("event.message_from").isNotNull()
                & F.col("event.message_to").isNotNull()
            )
            .select(
                F.col(message_from).alias("user_left"),
                F.col(message_to).alias("user_right"),
                F.col("lat").alias("lat_from"),
                F.col("lon").alias("lon_from"),
            )
            .distinct()
        )
    
    #определяем отправителей
    senders = finding_senders_and_recipients ("event.message_from", "event.message_to")

    #определяем получателей
    recipients = finding_senders_and_recipients ("event.message_to", "event.message_from")

    # пишем функцию, которая будет определять сообщения/подписки с временным форматом unix (секунды с начала эпохи)
    def events_unix (event_type):
        return (
            events.filter(F.col("event_type") == event_type)
            .where(
                F.col("lat").isNotNull() | (F.col("lon").isNotNull()) | (
                    F.unix_timestamp(
                        F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"
                    ).isNotNull()
                )
            )
            .select(
                F.col("event.message_from").alias("user_right"),
                F.col("lat").alias("lat"),
                F.col("lon").alias("lon"),
                F.unix_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss").alias("time")
            )
            .distinct()
        )

    #определяем сообщения
    events_messages_unix = events_unix("message")

    #определяем подписки
    events_subscriptions_unix = events_unix("subscription")
    
    w = Window.partitionBy("user_right")
    #пишем функцию, которая определяет последние сообщения/подписки
    def events_result (events_filtered_unix):
        return(
            events_filtered_unix.withColumn("maxdatetime", F.max("time").over(w))
            .where(F.col("time") == F.col("maxdatetime"))
            .select("user_right", "lat", "lon", "time")
        )
    
    #определяем последние сообщения
    events_messages = events_result(events_messages_unix)

    #определяем последние подписки
    events_subscriptions = events_result(events_subscriptions_unix)
    
    # объединяем последние подписки и сообщения
    events_coordinates = events_messages.union(events_subscriptions).distinct()
    
    # определяем пользователей контактировавших друг с другом
    users_intersection = (
        senders.union(recipients)
        .withColumn("arr", F.array_sort(F.array(*cols)))
        .drop_duplicates(["arr"])
        .withColumn("hash", F.hash(F.concat(F.col("user_left"), F.col("user_right"))))
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from", "hash")
    )

    # определяем пользователей, не имевших друг с другом контактов и подписанные на одни и те же каналы
    subscriptions_without_intersection = (
        subscriptions.join(
            users_intersection.withColumnRenamed(
                "user_right", "user_right_temp"
            ).withColumnRenamed("user_left", "user_left_temp"),
            on=["hash"],
            how="left",
        )
        .where(F.col("user_right_temp").isNull())
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right"))
        .select("user_left", "user_right", "lat_from", "lon_from")
    )
    
    # определяем последние координаты пользователей
    events_subscription_coordinates = (
        subscriptions_without_intersection.join(
            events_coordinates.withColumnRenamed(
                "user_id", "user_left"
            ).withColumnRenamed("lon", "lon_left")
            .withColumnRenamed("lat", "lat_left"),
            on=["user_right"],
            how="inner",
        ).join(
            events_coordinates.withColumnRenamed(
                "user_id", "user_right"
            ).withColumnRenamed("lon", "lon_right")
            .withColumnRenamed("lat", "lat_right"),
            on=["user_right"],
            how="inner",
        )
        #.select("user_right", "user_left", "lat_from", "lon_from", "lat_left", "lon_left", "time", "lat_right", "lon_right")
    )
    
    # определяем не контактирующих друг с другом пользователей в пределах одного города
    distance = (
        events_subscription_coordinates.withColumn(
            "distance",
            udf_func(
                F.col("lat_left"),
                F.col("lat_right"),
                F.col("lon_left"),
                F.col("lon_right"),
            ).cast(DoubleType()),
        )
        .where(F.col("distance") <= 1.0)
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_from", "lon_from", "distance")
        #.select("user_right", "user_left", "lat", "lon", "time", "lat_right", "lon_right", "time")
    )
    
    # определяем город
    users_city = (
        distance.crossJoin(cities.hint("broadcast"))
        .withColumn(
            "distance",
            udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(
                DoubleType()
            ),
        )
        .withColumn(
            "row",
            F.row_number().over(
                Window.partitionBy("user_left", "user_right").orderBy(
                    F.col("distance").asc()
                )
            ),
        )
        .filter(F.col("row") == 1)
        .drop("row", "lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city", "zone_id")
        .distinct()
        #.select("user_right", "user_left", "time", "lat_right", "lon_right", "time", "id", "zone_id", "lat_c", "lng_c", "timezone")
    )
    
    # рекоммендации по подпискам пользователей
    recommendations = (
        users_city.withColumn("processed_dttm", current_date())
        .withColumn(
            "local_datetime",
            F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")),
        )
        .withColumn("local_time", date_format(col("local_datetime"), "HH:mm:ss"))
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )
    
    # записывем в формате parquet..
    recommendations.write.mode("overwrite").parquet(
        f"{target_path}/mart/recommendations/"
    )          


if __name__ == "__main__":

    main()