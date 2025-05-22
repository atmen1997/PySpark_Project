from pyspark.sql.functions import col, explode, to_timestamp, to_date, hour, minute, round, count, broadcast
from pyspark.sql import Window
from pyspark.sql.functions import desc, row_number
from datetime import datetime

def process_raw_data(df):
    # Flatten nested fields and extract relevant columns
    df = (
        df.withColumn("sensordatavalues", explode("sensordatavalues"))
          .withColumn("value_type", col("sensordatavalues.value_type"))
          .withColumn("sensor_id", col("sensor.id"))
          .withColumn("sensor_type", col("sensor.sensor_type.name"))
          .withColumn("value", col("sensordatavalues.value"))
          .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("date", to_date(col("timestamp")))
          .withColumn("hour", hour(col("timestamp")))
          .withColumn("minute", minute(col("timestamp")))
          .withColumn("latitude", round(col("location.latitude").cast("float"),4))
          .withColumn("longitude", round(col("location.longitude").cast("float"),4))
          .withColumn("country_code", col("location.country"))
          .drop("sensordatavalues", "sampling_rate", "location")
    )

    # Filter out records with irrelevant value types and invalid coordinates
    df = df.filter((col("value_type") == "P1") | (col("value_type") == "P2"))
    df = df.filter((col("latitude") != 0) & (col("longitude") != 0))

    # Pivot to restructure the dataset
    df = df.groupBy("id", "timestamp", "date", "hour", "minute", "latitude", "longitude", "country_code", "sensor_id", "sensor_type")\
        .pivot("value_type")\
        .agg({"value": "first"})

    # Filter records where at least one of P1 or P2 is not null
    df = df.filter((col("P1").isNotNull()) | (col("P2").isNotNull()))

    # Cast P1 and P2 to the correct data type
    df = df.withColumn("P1", col("P1").cast("float")).withColumn("P2", col("P2").cast("float"))
    return df

def test_duplication(df):
    print("Testing for duplication...")
    df_count = (
        df.groupBy("sensor_id", "date")
          .agg(count("*").alias("count"))
          .filter(col("count") > 1)
    )
    print(f"Duplicate rows found: {df_count.count()}")
    df_count.show(10)
    return df_count.count()

def deduplicate_df(df):
    print("Deduplicating records...")
    # Define window for deduplication
    window_spec = Window.partitionBy("sensor_id", "date").orderBy(desc("timestamp"))
    df = df.withColumn("row_number", row_number().over(window_spec))
    df = df.filter(col("row_number") == 1).drop("row_number")
    print("Deduplication complete.")
    return df

def clean_df(df):
    start_time = datetime.now()
    print(f"Data cleaning started at {start_time}.")

    # Step 1: Process raw data
    df = process_raw_data(df)

    # Step 2: Check and handle duplication
    while test_duplication(df) > 0:
        df = deduplicate_df(df)

    end_time = datetime.now()
    print(f"Data cleaning finished at {end_time}. Duration: {end_time - start_time}.")
    return df