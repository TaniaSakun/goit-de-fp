from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os

def create_spark_session(app_name="SilverToGold"):
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_parquet(spark, path):
    """Read a Parquet file and return a DataFrame."""
    return spark.read.parquet(path)

def process_data(athlete_event_results_df, athlete_bio_df):
    """Process data by joining tables and aggregating statistics."""
    # Rename duplicating column for joining
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "b_country_noc")
    
    # Join tables on 'athlete_id'
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")
    
    # Aggregate statistics
    return (joined_df.groupBy("sport", "medal", "sex", "country_noc")
            .agg(avg("height").alias("avg_height"),
                 avg("weight").alias("avg_weight"),
                 current_timestamp().alias("timestamp")))

def save_parquet(df, output_path):
    """Save the DataFrame in Parquet format."""
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Successfully processed data and saved to {output_path}")

def main():
    spark = create_spark_session()
    
    # Read input data
    athlete_bio_df = read_parquet(spark, "silver/athlete_bio")
    athlete_event_results_df = read_parquet(spark, "silver/athlete_event_results")
    
    # Process data
    agg_df = process_data(athlete_event_results_df, athlete_bio_df)
    
    # Save results
    output_path = "gold/avg_stats"
    save_parquet(agg_df, output_path)
    
    # Display results
    spark.read.parquet(output_path).show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
