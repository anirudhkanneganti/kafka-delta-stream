from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaTest")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail_orders_us")
    .option("startingOffsets", "earliest")
    .load()
)

df.writeStream.format("console").start().awaitTermination()
