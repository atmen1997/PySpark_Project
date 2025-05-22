from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from src.data_load import load_data
from src.data_transform import clean_df
from src.calculations import cal_aqi
from src.assignments import task_1, task_2, task_3
from src.visualize import plot_map
from src.schema import define_schema
from src.config import OUTPUT_PATH_DATA
from datetime import datetime
import json

def main():
    # Step 1: Initialize Spark session
    thread = 4
    spark = SparkSession.builder \
                        .appName("AirQualityAnalysis") \
                        .master(f"local[{thread}]")\
                        .getOrCreate()
    # Set logging level to WARN and ERROR for clarity
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"Spark running with {thread} thread")
        # Log program start time
        start_time = datetime.now()
        print(f"Program start time: {start_time}")

        # Step 2: Load data
        print("Loading data...")
        data, file_names = load_data()
        file_names.sort()
        for file in file_names:
            print(f"File loaded: {file}")

        # Save raw data as JSON for backup
        with open(f"{OUTPUT_PATH_DATA}/data_24h_full.json", "w") as f:
            json.dump(data, f)
        
        # Step 3: Define schema
        print("Defining data schema...")
        schema = define_schema()

        # Step 4: Clean and preprocess data
        print("Loading data to spark dataframe...")
        raw_df = spark.createDataFrame(data, schema=schema)
        print("Number of partitions:", raw_df.rdd.getNumPartitions())
        # raw_df = spark.read.json(data, schema=schema) # Slow can not even read data
        print("Cleaning and transforming data...")
        cleaned_df = clean_df(raw_df)
        cleaned_pandas_df = cleaned_df.toPandas()
        cleaned_pandas_df.to_csv(f"{OUTPUT_PATH_DATA}/data_24h_cleaned.csv", index=False)
        print("Cleaned data saved as CSV.")
        
        # Step 5: Add country information for further analysis
        print("Adding country information...")
        country_code = spark.read.csv("data/country_info_data/country_info.csv", header=True)
        country_code = country_code.select(col("alpha-2"), col("name").alias("country_name"), col("region"), col("sub-region"))
        cleaned_df = cleaned_df.join(
            broadcast(country_code), cleaned_df["country_code"] == country_code["alpha-2"], "left"
        ).drop(country_code["alpha-2"])
        print("Country information added.")

        # Step 6: Calculate AQI
        print("Calculating AQI...")
        cleaned_with_aqi_df = cal_aqi(cleaned_df)
        cleaned_with_aqi_df.show(10)
        print("AQI calculation completed.")
        # Check summary of data after clean
        sensor_sum_df = cleaned_with_aqi_df.select(col("latitude"),col("longitude"),col("sensor_id")).distinct()
        print("Calculating count of the sensor...")
        print(sensor_sum_df.count())
        plot_map(sensor_sum_df, 'sensor location', 'sensor_id', False, True)

        # Task 1: Top 10 countries with air quality improvement
        print("Executing Task 1: Top 10 countries with AQI improvement...")
        top_10_improvement_df = task_1(cleaned_with_aqi_df)
        top_10_improvement_df.toPandas().to_csv(
            f"{OUTPUT_PATH_DATA}/task_1_top_10_improvement_df.csv", index=False
        )
        print("Task 1 results saved.")

        # Task 2: Top 50 AQI improvements by clustered region
        print("Executing Task 2: Top 50 AQI improvements by clustered region...")
        clustered_df, top_50_improvement_df = task_2(cleaned_with_aqi_df, location_type="optimal_clustered_region")
        clustered_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_2_clustered_df.csv", index=False)
        top_50_improvement_df.toPandas().to_csv(
            f"{OUTPUT_PATH_DATA}/task_2_top_50_improvement_df.csv", index=False
        )
        print("Task 2 results saved.")
    
        # Task 3: Longest streaks of good air quality by clustered region
        print("Executing Task 3: Longest streaks of good air quality...")
        streak_df, max_streak_df, streak_frequency_table = task_3(clustered_df, streak_of="sensor_id")
        streak_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_3_streak_df.csv", index=False)
        max_streak_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_3_max_streak_df.csv", index=False)
        streak_frequency_table.to_csv(
            f"{OUTPUT_PATH_DATA}/task_3_max_streaks_histogram_table.csv", index=False
        )
        print("Task 3 results saved.")
        
        # Log program end time and duration
        end_time = datetime.now()
        print(f"Program end time: {end_time}")
        print(f"Program run duration: {end_time - start_time}")
        print(f"Running with {thread} for {end_time - start_time}")
        
    finally:
        # Step 6: Stop the Spark session to release resources
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()



# # Testing with different thread.
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, broadcast
# from src.data_load import load_data
# from src.data_transform import clean_df
# from src.calculations import cal_aqi
# from src.assignments import task_1, task_2, task_3
# from src.visualize import plot_map
# from src.schema import define_schema
# from src.config import OUTPUT_PATH_DATA
# from datetime import datetime
# import json

# def main():
#     # Step 1: Initialize Spark session
#     thread = 6
#     thread_time = []
#     for i in range(thread):
#         spark = SparkSession.builder \
#                             .appName("AirQualityAnalysis") \
#                             .master(f"local[{i+1}]")\
#                             .getOrCreate()
#         # Set logging level to WARN and ERROR for clarity
#         spark.sparkContext.setLogLevel("WARN")
#         spark.sparkContext.setLogLevel("ERROR")

#         print(f"Spark running with {i+1} thread")
#         # Log program start time
#         start_time = datetime.now()
#         print(f"Program start time: {start_time}")

#         # Step 2: Load data
#         print("Loading data...")
#         data, file_names = load_data()
#         file_names.sort()
#         for file in file_names:
#             print(f"File loaded: {file}")

#         # Save raw data as JSON for backup
#         with open(f"{OUTPUT_PATH_DATA}/data_24h_full.json", "w") as f:
#             json.dump(data, f)
        
#         # Step 3: Define schema
#         print("Defining data schema...")
#         schema = define_schema()

#         # Step 4: Clean and preprocess data
#         print("Loading data to spark dataframe...")
#         raw_df = spark.createDataFrame(data, schema=schema)
#         # raw_df = spark.read.json(data, schema=schema) # Slow can not even read data
#         raw_df = raw_df.repartition(i+1,"id")
#         print("Number of partitions:", raw_df.rdd.getNumPartitions())
#         print("Cleaning and transforming data...")
#         cleaned_df = clean_df(raw_df)
#         cleaned_pandas_df = cleaned_df.toPandas()
#         cleaned_pandas_df.to_csv(f"{OUTPUT_PATH_DATA}/data_24h_cleaned.csv", index=False)
#         print("Cleaned data saved as CSV.")

#         # Step 5: Add country information for further analysis
#         print("Adding country information...")
#         country_code = spark.read.csv("data/country_info_data/country_info.csv", header=True)
#         country_code = country_code.select(col("alpha-2"), col("name").alias("country_name"), col("region"), col("sub-region"))
#         cleaned_df = cleaned_df.join(
#             broadcast(country_code), cleaned_df["country_code"] == country_code["alpha-2"], "left"
#         ).drop(country_code["alpha-2"])
#         print("Country information added.")

#         # Step 6: Calculate AQI
#         print("Calculating AQI...")
#         cleaned_with_aqi_df = cal_aqi(cleaned_df)
#         cleaned_with_aqi_df.show(10)
#         print("AQI calculation completed.")
#         # Check summary of data after clean
#         sensor_sum_df = cleaned_with_aqi_df.select(col("latitude"),col("longitude"),col("sensor_id")).distinct()
#         print("Calculating count of the sensor...")
#         print(sensor_sum_df.count())
#         # plot_map(sensor_sum_df, 'sensor location', 'sensor_id', False, True)

#         # # Task 1: Top 10 countries with air quality improvement
#         # print("Executing Task 1: Top 10 countries with AQI improvement...")
#         # top_10_improvement_df = task_1(cleaned_with_aqi_df)
#         # top_10_improvement_df.toPandas().to_csv(
#         #     f"{OUTPUT_PATH_DATA}/task_1_top_10_improvement_df.csv", index=False
#         # )
#         # print("Task 1 results saved.")

#         # # Task 2: Top 50 AQI improvements by clustered region
#         # print("Executing Task 2: Top 50 AQI improvements by clustered region...")
#         # clustered_df, top_50_improvement_df = task_2(cleaned_with_aqi_df, location_type="optimal_clustered_region")
#         # clustered_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_2_clustered_df.csv", index=False)
#         # top_50_improvement_df.toPandas().to_csv(
#         #     f"{OUTPUT_PATH_DATA}/task_2_top_50_improvement_df.csv", index=False
#         # )
#         # print("Task 2 results saved.")

#         # # Task 3: Longest streaks of good air quality by clustered region
#         # print("Executing Task 3: Longest streaks of good air quality...")
#         # streak_df, max_streak_df, streak_frequency_table = task_3(clustered_df, streak_of="sensor_id")
#         # streak_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_3_streak_df.csv", index=False)
#         # max_streak_df.toPandas().to_csv(f"{OUTPUT_PATH_DATA}/task_3_max_streak_df.csv", index=False)
#         # streak_frequency_table.to_csv(
#         #     f"{OUTPUT_PATH_DATA}/task_3_max_streaks_histogram_table.csv", index=False
#         # )
#         # print("Task 3 results saved.")
        
#         # Log program end time and duration
#         end_time = datetime.now()
#         print(f"Program end time: {end_time}")
#         duration = end_time - start_time
#         print(f"Program run duration: {duration}")
#         thread_time_dict = {i+1 : duration}
#         thread_time.append(thread_time_dict)
#     print(thread_time)
#     with open(f"{OUTPUT_PATH_DATA}/thread_time_check.txt","w") as f:
#         f.write(str(thread_time))
#         f.close()

# if __name__ == "__main__":
#     main()