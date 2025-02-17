import os
import requests
from pyspark.sql import SparkSession


BASE_URL = "https://ftp.goit.study/neoversity/"


def create_spark_session():
    """Creates and returns a Spark session."""
    return SparkSession.builder.appName("LandingToBronze").getOrCreate()


def download_data(table_name):
    """Downloads the CSV file for a given table from the FTP server."""
    local_file = f"{table_name}.csv"
    download_url = f"{BASE_URL}{local_file}"

    print(f"📥 Downloading from: {download_url}...")

    response = requests.get(download_url)

    if response.status_code == 200:
        with open(local_file, 'wb') as file:
            file.write(response.content)
        print(f"✅ File downloaded successfully: {local_file}")
    else:
        exit(f"❌ Failed to download {table_name}. Status code: {response.status_code}")


def process_table(spark, table_name):
    """Reads a CSV file, processes it, and saves it in Parquet format."""
    local_path = f"{table_name}.csv"
    output_path = os.path.join("bronze", table_name)

    print(f"🚀 Processing table: {table_name}...")

    # Read CSV
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Save table in Parquet format
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Successfully processed and saved: {output_path}")
    df.show(truncate=False)

if __name__ == "__main__":
    spark = create_spark_session()
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        download_data(table)
        process_table(spark, table)

    spark.stop()
