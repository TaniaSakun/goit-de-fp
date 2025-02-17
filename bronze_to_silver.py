from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os


def create_spark_session():
    """Creates and returns a Spark session."""
    return SparkSession.builder.appName("BronzeToSilver").getOrCreate()


def clean_text(text):
    """Removes unwanted characters from text fields."""
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


def process_table(spark, table):
    """Processes a single table from the bronze layer to the silver layer."""
    input_path = f"bronze/{table}"
    output_path = f"silver/{table}"

    print(f"Processing table: {table}...")

    # Read the table
    df = spark.read.parquet(input_path)

    # Define and register UDF for text cleaning
    clean_text_udf = udf(clean_text, StringType())

    # Clean text in all string columns
    text_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]  # ✅ Fixed here
    for col_name in text_columns:
        df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Remove duplicates
    df_cleaned = df.dropDuplicates()

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Save results
    df_cleaned.write.mode("overwrite").parquet(output_path)

    print(f"✅ Successfully processed and saved: {output_path}")
    df_cleaned.show(truncate=False)  # Display the cleaned DataFrame


if __name__ == "__main__":
    spark = create_spark_session()
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        process_table(spark, table)

    spark.stop()