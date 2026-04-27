# Шаблон SparkSession
## Запуск универсального SparkSession
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("SparkExample") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        ) \
    .getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")
spark.stop()
### Чтение и запись в S3

s3_path_regions = "s3a://prod/jdbc/regions/*.parquet"
dev_path = "s3a://dev/test_regions"

# Чтение parquet-файла
df = spark.read.parquet(s3_path_regions)
df.show() 

df.write.mode("overwrite").parquet(dev_path)
print("Датафрейм сохранен!")

### Прочитать файл локально и сохранить локально
import os
import subprocess

# Скачиваем файл
subprocess.run(
    'curl -L -o customs_data.csv "https://huggingface.co/datasets/halltape/customs_data/resolve/main/customs_data.csv?download=true"',
    shell=True,
    check=True
)

num_output_files = 6

df = spark.read.csv('customs_data.csv', header=True, sep=';')

output_path = f"coalesced_data_{num_output_files}"

# Записываем с нужным числом файлов
df.coalesce(num_output_files).write.mode("overwrite").csv(output_path)

# Функция для подсчёта размера всех файлов в папке
def get_folder_size_in_bytes(folder_path):
    total_size = 0
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            fp = os.path.join(root, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    return total_size

total_bytes = get_folder_size_in_bytes(output_path)

size_per_file = total_bytes / num_output_files if num_output_files > 0 else 0

total_mb = total_bytes / 1024 / 1024
size_per_file_mb = size_per_file / 1024 / 1024

print(f"Общий размер всех файлов: {total_mb:.2f} Mb")
print(f"Примерный размер одного файла из {num_output_files}: {size_per_file_mb:.2f} Mb")

### Чтение и запись в Clickhouse
# ⬇️ Параметры подключения к CLICKHOUSE
jdbc_url = 'jdbc:clickhouse://clickhouse01:8123/default'
jdbc_url_dev = 'jdbc:clickhouse://clickhouse01:8123/dev'
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = 'enriched_earthquakes'


# Чтение таблицы из ClickHouse
enriched = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .load()

enriched.show()


enriched.write \
    .format("jdbc") \
    .option("url", jdbc_url_dev) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("createTableOptions", """
            ENGINE = ReplacingMergeTree(updated_at)
            PARTITION BY toYYYYMM(load_date)
            ORDER BY (load_date, id)
        """) \
    .mode("append") \
    .save()

print("Таблица сохранена в Clickhouse")
### Чтение и запись в PostgreSQL
import os
# ⬇️ Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
table_name = "public.regions"
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")


jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", table_name) \
    .option("fetchsize", 1000) \
    .option("driver", "org.postgresql.Driver") \
    .load()


jdbc_df.show()


# Запись в dev
target_url = "jdbc:postgresql://postgres_source:5432/dev"
target_table = "public.test_regions"

jdbc_df.write \
    .format("jdbc") \
    .option("url", target_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("dbtable", target_table) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Данные записаны в dev.public.test_regions")
### Чтение из Kafka
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import from_json

kafka_topic = "source.public.order_events"
kafka_bootstrap = "kafka:29092"


# Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()


# Описание схемы JSON сообщения
schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("ts", LongType(), True)
    ]), True),
    StructField("source", StructType([]), True),  # если не используешь, можно пустым
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])


# Распарсенные данные
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .where("data.after IS NOT NULL") \
    .select("data.after.*")

# Вывод в консоль
json_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()

# Чтение из PG и трансформация на Spark SQL
import os
# ⬇️ Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres_source:5432/source"
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

shops_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", "public.shops") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()

shop_tz_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("dbtable", "public.shop_timezone") \
            .option("fetchsize", 1000) \
            .option("driver", "org.postgresql.Driver") \
            .load()


# SQL от Аналитика
sql = """
    with sz as(SELECT
                cast(plant as INT) as id,
                time_zone
            FROM shop_timezone
            where cast(plant as INT) IS NOT NULL)
    , s as(
        select
            cast(st_id AS INT) as st_id,
            shop_name
        from shops)
    , cte as(
        select 
            *,
            row_number() over(PARTITION BY st_id ORDER BY st_id) as rnk
        from s join sz ON s.st_id = sz.id
    )
    select
        st_id,
        shop_name,
        CAST(
            CASE
                WHEN time_zone = '' OR time_zone IS NULL THEN 3
                ELSE substr(time_zone, 4)
            END
        AS INT) AS tz_code
    from cte
    where rnk = 1
"""
# Чтение из PG и трансформация на Spark SQL с TEMP View
# Регистрируем DataFrame-ы как временные вьюхи
shops_df.createOrReplaceTempView("shops")
shop_tz_df.createOrReplaceTempView("shop_timezone")

# Приводим plant / st_id к одному типу и готовим поле с числом часового пояса
spark.sql("""
    SELECT
        CAST(plant AS INT)           AS st_id,
        time_zone,
        SUBSTR(time_zone, 5)         AS int_time_zone
    FROM shop_timezone
    WHERE CAST(plant AS INT) IS NOT NULL
""").createOrReplaceTempView("sz")

spark.sql("""
    SELECT
        CAST(st_id AS INT) AS st_id,
        shop_name
    FROM shops
""").createOrReplaceTempView("s")

# Выбираем по магазину первую запись (с непустым time_zone идёт раньше)
spark.sql("""
    SELECT
        s.st_id,
        s.shop_name,
        sz.int_time_zone,
        ROW_NUMBER() OVER (
            PARTITION BY s.st_id
            ORDER BY
                CASE WHEN sz.time_zone = '' OR sz.time_zone IS NULL THEN 1 ELSE 0 END,
                s.st_id
        ) AS rn
    FROM s
    JOIN sz ON s.st_id = sz.st_id
""").createOrReplaceTempView("cte")

# Финальный результат
spark.sql("""
    SELECT
        st_id,
        shop_name,
        CAST(
            CASE
                WHEN int_time_zone = '' OR int_time_zone IS NULL THEN 3
                ELSE int_time_zone
            END AS INT
        ) AS tz_code
    FROM cte
    WHERE rn = 1
""").show()

# Альтернативный вариант использования SparkSQL

# Регистрируем DataFrame-ы как временные вьюхи
shops_df.createOrReplaceTempView("shops")
shop_tz_df.createOrReplaceTempView("shop_timezone")

spark.sql("""
    with group_data as (
        select plant, 
            substr(time_zone, 5) as int_time_zone, 
            shop_name, 
            row_number() over(partition by st_id order by 
                                                    (case when time_zone = '' or time_zone is null then 1 else 0 end), st_id) as rn
        from shop_timezone st join shops s on st.plant = cast(s.st_id as string)
        )
        select plant, shop_name, case 
                        when int_time_zone = '' 
                        then 3 
                        else cast(int_time_zone as int)
                    end as tz_code
        from group_data
        where rn = 1
""").show()
# Чтение из PG и трансформация на Spark API
from pyspark.sql.functions import col, when, substring

sz =(
        shop_tz_df
        .where(""" cast(plant as INT) IS NOT NULL """)
        .select(
            col("plant").cast("int").alias("id"),
            "time_zone")
    )
                
s = shops_df.select(col("st_id").cast("int").alias("st_id"), "shop_name")


from pyspark.sql.functions import row_number
from pyspark.sql import Window
window_spec = Window.partitionBy("st_id").orderBy(
    when((col("time_zone") == "") | (col("time_zone").isNull()), 1).otherwise(0),
    col("st_id")
)

cte = sz \
        .join(s, sz.id==s.st_id, "inner") \
        .select("*") \
        .withColumn("rank", row_number().over(window_spec))


final_df = (
        cte
        .where(""" rank = 1 """)
        .select("st_id",
                "shop_name",
                "time_zone")
        .withColumn("tz_code",
                when((col("time_zone") == "") | (col("time_zone").isNull()), 3)
                .otherwise(substring(col("time_zone"), 5, 10).cast("int").cast("int")))
        .drop("time_zone")
)

final_df.show()

# CREATE DATABASE IF NOT EXISTS vindyukov ON CLUSTER '{cluster}';

# CREATE TABLE IF NOT EXISTS vindyukov.shops_local ON CLUSTER '{cluster}' (
#           st_id Int64,
#           shop_name String,
#           tz_code Int64
#       )
#  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/shops_local', '{replica}')
#  ORDER BY (st_id);

# CREATE TABLE IF NOT EXISTS vindyukov.shops ON CLUSTER '{cluster}' AS vindyukov.shops_local
# ENGINE = Distributed('{cluster}', 'vindyukov', 'shops_local', rand());

# TRUNCATE TABLE vindyukov.shops_local ON CLUSTER '{cluster}';

# ⬇️ Параметры подключения к CLICKHOUSE
# jdbc_url = 'jdbc:clickhouse://clickhouse01:8123/vindyukov'
# db_user = os.getenv('CLICKHOUSE_USER')
# db_password = os.getenv('CLICKHOUSE_PASSWORD')
# table_name = 'shops'

# final_df.write \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("user", db_user) \
#     .option("password", db_password) \
#     .option("dbtable", table_name) \
#     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
#     .mode("append") \
#     .save()

# print("Таблица сохранена в Clickhouse")
# Чтение из S3 - Transform Spark API - CH
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, arrays_zip, col, current_timestamp, date_format, from_unixtime, unix_timestamp
from pyspark.sql.types import ArrayType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("S3Example") \
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2,"
        "org.postgresql:postgresql:42.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")

# ⬇️ Параметры подключения к CLICKHOUSE
jdbc_url_dev = 'jdbc:clickhouse://clickhouse01:8123/shustikov'
db_user = os.getenv('CLICKHOUSE_USER')
db_password = os.getenv('CLICKHOUSE_PASSWORD')
table_name = 'weather_temp_rain'

s3_path_weather = "s3a://dev/api/halltape_vindyukov/*.parquet"

df = (spark
    .read
    .parquet(s3_path_weather)
    )

renamed_df = (df
                .withColumnRenamed("hourly.time", "hourly_time")
                .withColumnRenamed("hourly.temperature_2m", "hourly_temperature_2m")
                .withColumnRenamed("hourly.rain", "hourly_rain")
)

exploded_df = (renamed_df
                .select(
                    explode(arrays_zip("hourly_time", "hourly_temperature_2m", "hourly_rain")).alias("zipped"))
)


parsed_df = (exploded_df
                .select(
                    col("zipped.hourly_time").cast("timestamp").alias("time"),
                    col("zipped.hourly_temperature_2m").alias("temperature"),
                    col("zipped.hourly_rain").alias("rain"))
                .withColumn("load_date", col("time").cast("date"))
                .withColumn("updated_at", from_unixtime(unix_timestamp(current_timestamp())).cast("timestamp"))
            )

parsed_df = (exploded_df
                .select(
                    col("zipped.hourly_time").cast("timestamp").alias("time"),
                    col("zipped.hourly_temperature_2m").alias("temperature"),
                    col("zipped.hourly_rain").alias("rain"))
                .withColumn("load_date", col("time").cast("date"))
                .withColumn("updated_at", from_unixtime(unix_timestamp(current_timestamp())).cast("timestamp"))
            )

parsed_df.show()

# CREATE TABLE IF NOT EXISTS vindyukov.weather_temp_rain_local (
#     time DateTime,
#     temperature Float32,
#     rain Float32,
#     load_date Date,
#     updated_at DateTime,
#     id UInt64
# ) ENGINE = ReplacingMergeTree(updated_at)
# PARTITION BY toYYYYMM(load_date)
# ORDER BY (time)

# CREATE TABLE IF NOT EXISTS vindyukov.weather_temp_rain ON CLUSTER '{cluster}' AS vindyukov.weather_temp_rain_local
# ENGINE = Distributed('{cluster}', 'vindyukov', 'weather_temp_rain_local', rand());


# (
# parsed_df.write \
#     .format("jdbc") \
#     .option("url", jdbc_url_dev) \
#     .option("user", db_user) \
#     .option("password", db_password) \
#     .option("dbtable", table_name) \
#     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
#     .mode("append") \
#     .save()
# )

# print("Таблица сохранена в Clickhouse")