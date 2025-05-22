# Datetime for Timestamping
from datetime import datetime
# PySpark SQL Functions
from pyspark.sql.functions import collect_list, col, min, max
# Functions from Other Modules
from src.calculations import calculate_aqi_improvement, cluster_region, calculate_streak, calculate_max_streak, calculate_optimal_k
from src.visualize import plot_histogram, histogram_to_table, plot_map, plot_elbow_curve

def task_1(df):
    start_time = datetime.now()
    print(f"Task 1 started at: {start_time}")

    # Calculate AQI improvement by country
    print("Calculating top 10 AQI improvements...")
    result_df = calculate_aqi_improvement(df, "country_name")
    result_df.show(10)

    end_time = datetime.now()
    print(f"Task 1 finished at: {end_time}, Duration: {end_time - start_time}")
    return result_df

def task_2(df, location_type):
    start_time = datetime.now()
    print(f"Task 2 started at: {start_time}")

    # Perform initial clustering with K=200
    print("Performing initial clustering with K=200...")
    clustered_df = cluster_region(df, k=200)
    plot_map(clustered_df, "clustered region")
    print("Initial cluster map created.")

    # Determine optimal K using the elbow method
    print("Calculating optimal K using the elbow method...")
    wcss, optimal_k = calculate_optimal_k(df, max_k=10)
    print(f"Optimal K determined: {optimal_k}")
    plot_elbow_curve(wcss, optimal_k)

    # Re-cluster using optimal K
    optimal_clustered_df = cluster_region(df, optimal_k)
    plot_map(optimal_clustered_df, "clustered optimal region","clustered_region",True)
    optimal_clustered_df = optimal_clustered_df.select(
        col("id"), col("clustered_region").alias("optimal_clustered_region")
    )
    
    # Join and calculate AQI improvements by region
    print("Calculating AQI improvement of top 50 clustered region...")
    clustered_df = clustered_df.join(optimal_clustered_df, "id", "left")

    # Get yesterday and today comparision of region
    region_compare_df = calculate_aqi_improvement(clustered_df,"clustered_region")
    
    # Get country in region
    distinct_region_country_df = clustered_df.select(col("clustered_region"),col(location_type))\
                                   .distinct().orderBy(col("clustered_region"),ascending=True)
    # Group all country per region
    region_countries_df = distinct_region_country_df.groupBy("clustered_region").agg(collect_list(location_type)\
                                                    .alias(f"{location_type}s"))

    # Join with df to show countries for each region
    result_df = region_compare_df.join(region_countries_df, "clustered_region", "left") \
                          .drop(region_countries_df.clustered_region)
    result_df = result_df.select(col("rank"),col("clustered_region"),col(f"{location_type}s").cast("string"),col("previous_date"),
                   col("current_date"),col("previous_avg_aqi"),col("current_avg_aqi"),col("improvement")).orderBy("rank", ascending=True)
    result_df.show(50,truncate=False, vertical=False)
    end_time = datetime.now()
    end_time = datetime.now()
    print(f"Task 2 finished at: {end_time}, Duration: {end_time - start_time}")
    return clustered_df, result_df

def task_3(df, streak_of):
    start_time = datetime.now()
    print(f"Task 3 started at: {start_time}")

    # Calculate streaks and longest streaks
    print("Calculating streaks of good air quality...")
    streak_df = calculate_streak(df, streak_of)

    print("Calculating longest streaks...")
    max_streak_df = calculate_max_streak(streak_df, streak_of)

    # Plot histogram and create a frequency table
    print("Creating histogram of longest streaks...")
    plot_histogram(max_streak_df)
    streak_frequency_table = histogram_to_table(max_streak_df)
    from_date = df.select(min("date").alias("max_date")).collect()[0][0]
    to_date = df.select(max("date").alias("max_date")).collect()[0][0]
    period = (to_date - from_date).days
    print(f"Histogram and frequency table created over {period} days.")
    print(streak_frequency_table)
    print(f"The data was collected from {from_date} to {to_date} over a period  of {period} days")
    plot_histogram(max_streak_df)
    max_streak = max_streak_df.select(max(col("max_streak"))).collect()[0][0]
    max_streak_coordinate_df = streak_df.select(col("latitude"),col("longitude"),col("optimal_clustered_region")).filter(col("streak") == max_streak)
    plot_map(max_streak_coordinate_df, "optimal clustered region of max streak","optimal_clustered_region",True)
    end_time = datetime.now()
    print(f"Task 3 finished at: {end_time}, Duration: {end_time - start_time}")
    return streak_df, max_streak_df, streak_frequency_table
