from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# check agian with spark
SOURCE_TOPIC = "Financial_Transactions"
KAFKA_BROKER = "localhost:29092,localhost:39092,localhost:49092"
AGGREGATES_TOPIC = "Transaction_aggregates"
ANOMALIES_TOPIC = "Transaction_anomalies"
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-states'
spark = (SparkSession.builder
             .appName("FinancialTransactionProcessor")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
             .config("spark.sql.shuffle.partitions", 20)
             .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

    # Define Schema
transaction_schema = StructType([
        StructField('transactionId', StringType(), True),
        StructField('userId', StringType(), True),
        StructField('merchantId', StringType(), True),
        StructField('amount', DoubleType(), True),
        StructField('transactionTime', LongType(), True),
        StructField('transactionType', StringType(), True),
        StructField('location', StringType(), True),
        StructField('paymentMethod', StringType(), True),
        StructField('isInternational', StringType(), True),
        StructField('currency', StringType(), True)
    ])

# Read Kafka Stream
kafka_stream = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BROKER)
                    .option("subscribe", SOURCE_TOPIC)
                    .option("startingOffsets", "earliest")
                    .load())

    # Deserialize JSON
transaction_df = kafka_stream.selectExpr("CAST(value AS STRING)")\
                      .select(from_json(col("value"), transaction_schema).alias("data"))\
                      .select("data.*")
#Convert transaction time to timestamp
transaction_df = transaction_df.withColumn("transactionTimestamp", (col("transactionTime") / 1000).cast("timestamp"))

aggregate_df = transaction_df.groupBy(col("merchantId")) \
        .agg(sum("amount").alias("totalAmount"),
             count("*").alias("transactionCount"))

# Convert Aggregates to JSON and Send to Kafka
aggregate_query = aggregate_df.withColumn("key", col("merchantId").cast("string"))\
                    .withColumn("value", to_json(struct(
                        col("merchantId"), col("totalAmount"), col("transactionCount")
                    ))).selectExpr("key", "value")\
                    .writeStream\
                    .format("kafka")\
                    .outputMode("update")\
                    .option("kafka.bootstrap.servers", KAFKA_BROKER)\
                    .option("topic", AGGREGATES_TOPIC)\
                    .option("checkpointLocation", CHECKPOINT_DIR)\
                    .start().awaitTermination()



