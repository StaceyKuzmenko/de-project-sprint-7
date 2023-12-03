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

    spark = (
        SparkSession.builder.master("local")
        .appName("Learning DataFrames")
        .getOrCreate()
    )

    paths = input_paths(date, depth, events_path)

    events = spark.read.option("basePath", events_path).parquet(*paths)

    # пишем функцию, которая вычисляет датасеты относительно типа события (сообщения/реакции/подписки)    
    def events_types(event_type):    
        return (
            events.where(F.col("event_type") == event_type)
            .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
            .select(
                F.col("event.message_from").alias("user_id"),
                F.col("event.message_id").alias("message_id"),
                "lat",
                "lon",
                F.to_date(F.coalesce(F.col("event.datetime"), F.col("event.message_ts")),
                ).alias("date")
            )
        )
    # вычисляем датасет со всеми сообщениями
    events_messages = events_types("message")

    # вычисляем датасет со всеми реакциями
    events_reactions = events_types("reaction") 

    # вычисляем датасет со всеми подписками
    events_subscriptions = events_types("subscription")
    
    #добавим наименования городов к получившимся датасетам
    all_messages = events_messages.crossJoin(cities.hint("broadcast")).select("user_id", "message_id", "lat", "lon", "date", "city", "lat_c", "lng_c", "timezone")
    all_reactions = events_reactions.crossJoin(cities.hint("broadcast")).select("user_id", "message_id", "lat", "lon", "date", "city", "lat_c", "lng_c", "timezone")
    all_subscriptions = events_subscriptions.crossJoin(cities.hint("broadcast")).select("user_id", "message_id", "lat", "lon", "date", "city", "lat_c", "lng_c", "timezone")    
    
    #добавим столбец с дистанцией (по координатам пользователей) к получившимся выше датасетам
    messages_distances = all_messages.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )

    reactions_distances = all_reactions.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )

    subscriptions_distances = all_subscriptions.withColumn(
        "distance",
        udf_func(F.col("lat"), F.col("lat_c"), F.col("lon"), F.col("lng_c")).cast(DoubleType())
    )
    
    #пишем функцию, которая определяет координаты города, из которого конктерный пользователь отправил последнее сообщение/реакцию/подписку
    def latest_event (event_dataset):
        return ( 
            event_dataset.withColumn(
                "row",
                F.row_number().over(
                    Window.partitionBy("user_id", "date", "message_id").orderBy(F.col("date").asc())
                ),
            )
            .filter(F.col("row") == 1)
            .select("user_id", "message_id", "date", "city")
            .withColumnRenamed("city", "zone_id")
        )   

    #определяем координаты города, из которого конктерный пользователь отправил последнее сообщение
    latest_message_dataset = latest_event(messages_distances)

    #определяем координаты города, из которого конктерный пользователь отправил последнее сообщение
    latest_reaction_dataset = latest_event(reactions_distances)

    #определяем координаты города, из которого конктерный пользователь отправил последнее сообщение
    latest_subscription_dataset = latest_event(subscriptions_distances)    
    
    # расчёт всех сообщений по определённому выше городу с разбивкой по неделям и месяцам
    def count_events (dataset, week_action, month_action):
        return (
            dataset.withColumn("month", month(F.col("date")))
            .withColumn(
                "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
            )
            .withColumn(
                week_action,
                (F.count("message_id").over(Window.partitionBy("zone_id", "week"))),
            )
            .withColumn(
                month_action,
                (F.count("message_id").over(Window.partitionBy("zone_id", "month"))),
            )
            .select("zone_id", "week", "month", week_action, month_action)
            .distinct()
        )
    
    #считаем количество сообщений
    count_messages = count_events(latest_message_dataset, "week_message", "month_message")

    #считаем количество реакций
    count_reactions = count_events(latest_reaction_dataset, "week_reaction", "month_reaction")

    #считаем количество подписок
    count_subscriptions = count_events(latest_subscription_dataset, "week_subscription", "month_subscription")

    #рассчитаем количество регистраций
    count_registrations = (
        latest_message_dataset.withColumn("month", month(F.col("date")))
        .withColumn(
            "week", F.weekofyear(F.to_date(F.to_timestamp(F.col("date")), "yyyy-MM-dd"))
        )
        .withColumn(
            "row",
            (
                F.row_number().over(
                    Window.partitionBy("user_id").orderBy(F.col("date").asc())
                )
            ),
        )
        .filter(F.col("row") == 1)
        .withColumn(
            "week_user", (F.count("row").over(Window.partitionBy("zone_id", "week")))
        )
        .withColumn(
            "month_user", (F.count("row").over(Window.partitionBy("zone_id", "month")))
        )
        .select("zone_id", "week", "month", "week_user", "month_user")
        .distinct()
    )

    #рассчитаем итоговую витрину
    geo_mart = (
        count_messages.join(count_registrations, ["zone_id", "week", "month"], how="full")
        .join(count_reactions, ["zone_id", "week", "month"], how="full")
        .join(count_subscriptions, ["zone_id", "week", "month"], how="full")
    )

    # Заполняем пропуски нулями
    geo_mart = geo_mart.fillna(0)  
    
    geo_mart.write.mode("overwrite").parquet(f"{target_path}/mart/geo")



if __name__ == "__main__":

    main()