import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from configs import config

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Packages for PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4 "
    "pyspark-shell"
)

# Spark Session Initialization
spark = (
    SparkSession.builder
    .appName("JDBCToKafka")
    .master("local[*]")
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)


def read_from_mysql(jdbc_url, table_name):
    """Reads data from MySQL into a DataFrame."""
    try:
        df = (
            spark.read.format("jdbc")
            .options(
                url=jdbc_url,
                driver="com.mysql.cj.jdbc.Driver",
                dbtable=table_name,
                user=config["mysql_username"],
                password=config["mysql_password"],
            )
            .load()
        )
        logging.info(f"Successfully read data from MySQL table: {table_name}")
        return df
    except Exception as e:
        logging.error(f"Failed to read from MySQL table {table_name}: {e}")
        return None


def write_to_kafka(df, topic):
    """Writes a DataFrame to a Kafka topic."""
    try:
        (
            df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
            .write.format("kafka")
            .option("kafka.bootstrap.servers", config["bootstrap_servers"])
            .option("kafka.security.protocol", config["security_protocol"])
            .option("kafka.sasl.mechanism", config["sasl_mechanism"])
            .option(
                "kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username={config['username']} password={config['password']};",
            )
            .option("topic", topic)
            .save()
        )
        logging.info(f"Successfully wrote data to Kafka topic: {topic}")
    except Exception as e:
        logging.error(f"Error writing to Kafka topic {topic}: {e}")


def read_from_kafka(topic, schema):
    """Reads data from a Kafka topic into a DataFrame."""
    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config["bootstrap_servers"])
            .option("kafka.security.protocol", config["security_protocol"])
            .option("kafka.sasl.mechanism", config["sasl_mechanism"])
            .option(
                "kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username={config['username']} password={config['password']};",
            )
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "5")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
        )
        logging.info(f"Successfully started streaming from Kafka topic: {topic}")
        return df
    except Exception as e:
        logging.error(f"Error reading from Kafka topic {topic}: {e}")
        return None


def write_to_mysql(df, jdbc_url, table_name):
    """Writes a DataFrame to a MySQL table."""
    try:
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", config["mysql_username"])
            .option("password", config["mysql_password"])
            .mode("append") 
            .save()
        )
        logging.info(f"Successfully wrote data to MySQL table: {table_name}")
    except Exception as e:
        logging.error(f"Error writing to MySQL table {table_name}: {e}")


def foreach_batch_function(batch_df, epoch_id):
    """Processes each micro-batch: writes to Kafka and MySQL."""
    write_to_kafka(batch_df, topic_2)
    write_to_mysql(batch_df, jdbc_url_w, "tetiana_sakun_athlete_enriched_agg")


# MySQL Configs
jdbc_url_r = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_url_w = "jdbc:mysql://217.61.57.46:3306/neo_data"

# Kafka Topic Names
my_name = config["my_name"]
topic_1 = f"{my_name}_athlete_event_results"
topic_2 = f"{my_name}_enriched_athlete_avg"

# Step 1: Read and Filter Athlete Bio Data
athlete_bio_df = read_from_mysql(jdbc_url_r, "athlete_bio")
if athlete_bio_df:
    athlete_bio_df = athlete_bio_df.filter(
        (col("height").isNotNull()) & (col("weight").isNotNull()) &
        (col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull())
    )
else:
    logging.error("Failed to read athlete_bio. Exiting...")
    exit(1)

# Step 2: Read Athlete Event Results
athlete_event_df = read_from_mysql(jdbc_url_r, "athlete_event_results")
if athlete_event_df:
    write_to_kafka(athlete_event_df, topic_1)
else:
    logging.error("Failed to read athlete_event_results. Exiting...")
    exit(1)

# Step 3: Read Event Results from Kafka
schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True),
])

event_results_df = read_from_kafka(topic_1, schema)
if event_results_df:
    event_results_df = event_results_df.select("data.athlete_id", "data.sport", "data.medal")
else:
    logging.error("Failed to stream from Kafka. Exiting...")
    exit(1)

# Step 4: Join Data and Compute Aggregates
joined_df = (
    athlete_bio_df.join(event_results_df, on="athlete_id")
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp"),
    )
)

# Step 5: Stream to Kafka and MySQL
(
    joined_df.writeStream
    .outputMode("update")
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", "/tmp/checkpoints-stream")
    .start()
    .awaitTermination()
)