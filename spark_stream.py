from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# ---------------------
# Create Spark Session
# ---------------------
spark = SparkSession.builder \
    .appName("WeatherKafkaToPostgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------
# Kafka Stream
# ---------------------
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bamauli")
        .option("startingOffsets", "latest")
        .load()
)

df1 = df.selectExpr("CAST(value AS STRING)")

# ---------------------
# Schema
# ---------------------
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType()),
        StructField("lat", FloatType())
    ])),
    StructField("main", StructType([
        StructField("temp", FloatType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("name", StringType())
])

json_df = df1.select(from_json(col("value"), schema).alias("data"))

clean_df = json_df.select(
    col("data.coord.lon").alias("lon"),
    col("data.coord.lat").alias("lat"),
    col("data.main.temp").alias("temperature"),
    col("data.main.humidity").alias("humidity"),
    col("data.name").alias("city")
)

# ---------------------
# PostgreSQL Setup
# ---------------------
db_url = "jdbc:postgresql://localhost:5432/clickstream_db"
db_table = "weather_stream"
db_user = "postgres"
db_password = "Bamauli@8171"

# ---------------------
# Write to PostgreSQL Function
# ---------------------
def write_to_pg(batch_df, batch_id):
    batch_df = batch_df.withColumn("batch_time", current_timestamp())

    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# ---------------------
# Start Stream
# ---------------------
query = (
    clean_df.writeStream
        .foreachBatch(write_to_pg)
        .outputMode("update")
        .start()
)

query.awaitTermination()
