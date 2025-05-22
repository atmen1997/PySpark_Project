# PySpark SQL Functions:
from pyspark.sql.functions import col, when, greatest, date_sub, max, avg, row_number, lag, sum, round
# PySpark Window Specification:
from pyspark.sql import Window
# Machine Learning Libraries for Clustering:
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from kneed import KneeLocator 


# Function to calculate AQI according to the assignment
def cal_aqi(df):
    df = df.withColumn("aqi",
                    greatest(
                             when((col("p1") >=  0) & (col("p1") < 17), 1)
                            .when((col("p1") >= 17) & (col("p1") < 34), 2)
                            .when((col("p1") >= 34) & (col("p1") < 51), 3)
                            .when((col("p1") >= 51) & (col("p1") < 59), 4)
                            .when((col("p1") >= 59) & (col("p1") < 67), 5)
                            .when((col("p1") >= 67) & (col("p1") < 76), 6)
                            .when((col("p1") >= 76) & (col("p1") < 84), 7)
                            .when((col("p1") >= 84) & (col("p1") < 92), 8)
                            .when((col("p1") >= 92) & (col("p1") <= 100), 9)
                            .when((col("p1") > 100), 10)
                            .otherwise(None)
                            ,when((col("p2") >= 0) & (col("p2") <= 11), 1)
                            .when((col("p2") >= 12) & (col("p2") <= 23), 2)
                            .when((col("p2") >= 24) & (col("p2") <= 35), 3)
                            .when((col("p2") >= 36) & (col("p2") <= 41), 4)
                            .when((col("p2") >= 42) & (col("p2") <= 47), 5)
                            .when((col("p2") >= 48) & (col("p2") <= 53), 6)
                            .when((col("p2") >= 54) & (col("p2") <= 58), 7)
                            .when((col("p2") >= 59) & (col("p2") <= 64), 8)
                            .when((col("p2") >= 65) & (col("p2") <= 70), 9)
                            .when((col("p2") > 70), 10)
                            .otherwise(None)
                            ))\
          .withColumn("Range",
                            when((col("aqi") ==1) | (col("aqi") == 2) | (col("aqi") == 3), "Low")
                            .when((col("aqi") ==4) | (col("aqi") == 5) | (col("aqi") == 6), "Medium")
                            .when((col("aqi") ==7) | (col("aqi") == 8) | (col("aqi") == 9), "High")
                            .when((col("aqi") ==10), "Very High")
                            .otherwise(None)
                            )
    return df


def calculate_aqi_improvement(df, pk):
    # Get current date and previous date
    previous_date = df.select(date_sub(max("date"), 1).alias("previous_date")).collect()[0][0]
    current_date = df.select(max("date").alias("current_date")).collect()[0][0]

    # Get previous date average aqi
    previous_result_df = df.groupBy(col(pk).alias(f"pre_{pk}"),col("date").alias("previous_date")) \
                              .agg(avg("aqi")).filter(col("previous_date") == previous_date)
    previous_result_df = previous_result_df.withColumnRenamed("avg(aqi)","previous_avg_aqi")

    # Get current date average aqi
    current_result_df = df.groupBy(col(pk).alias(f"cur_{pk}"),col("date").alias("current_date")) \
                                .agg(avg("aqi")).filter(col("current_date") == current_date)
    current_result_df = current_result_df.withColumnRenamed("avg(aqi)","current_avg_aqi")

    # Round average AQI
    previous_result_df = previous_result_df.withColumn("previous_avg_aqi", round(col("previous_avg_aqi"),2))
    current_result_df =  current_result_df.withColumn("current_avg_aqi", round(col("current_avg_aqi"),2))

    # Joining the results
    result_df = previous_result_df.join(current_result_df, on= previous_result_df[f"pre_{pk}"] == current_result_df[f"cur_{pk}"] \
                                        , how="inner")
    result_df = result_df.select(col("previous_date"),
                                 col("current_date"),
                                 col(f"pre_{pk}").alias(pk),
                                 col("previous_avg_aqi"),
                                 col("current_avg_aqi")
                                 )
    
    result_df = result_df.withColumn("improvement", round(col("current_avg_aqi") - col("previous_avg_aqi"),2))
    window_spec = Window.partitionBy().orderBy(col("improvement").desc())
    ranked_result_df = result_df.withColumn("rank", row_number().over(window_spec))
    final_result_df = ranked_result_df.select("rank",pk,"previous_date","current_date","previous_avg_aqi","current_avg_aqi","improvement")
    final_result_df = ranked_result_df.select(col("rank"),col(pk),col("previous_date"),col("current_date"),\
                                             col("previous_avg_aqi"),col("current_avg_aqi"),col("improvement"))
    return final_result_df


def cluster_region(df,k):
    # Prepare data with VectorAssembler
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    df_kmeans = assembler.transform(df)

    # Apply K-means clustering
    kmeans = KMeans(featuresCol="features", predictionCol="clustered_region", k=k, seed=1)
    model = kmeans.fit(df_kmeans)

    # Make predictions
    predictions = model.transform(df_kmeans)
    # predictions.drop("features")
    return predictions


def calculate_optimal_k(df, max_k):
    # Prepare data with VectorAssembler
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    df_kmeans = assembler.transform(df)

    wcss = []  # List to store WCSS values

    for k in range(2, max_k + 1):
        kmeans = KMeans(featuresCol="features", predictionCol="clustered_region", k=k, seed=1)
        model = kmeans.fit(df_kmeans)
        wcss.append(model.summary.trainingCost)  # Append WCSS for the current k
        print("Testing with k =",k)

    # Find the elbow point
    knee_locator = KneeLocator(range(2, max_k + 1), wcss, curve="convex", direction="decreasing")
    optimal_k = knee_locator.knee
    return wcss, optimal_k


def calculate_streak(df, streak_of):
    # Define the window specification
    df = df.withColumn("is_good", when(col("range") == "Low", 1).otherwise(0))
    windowSpec = Window.partitionBy(streak_of).orderBy("timestamp")
    # Use lag to shift is_good column and identify reset points
    df = df.withColumn("prev_is_good", lag("is_good").over(windowSpec))
    # Check if the streak is reset or not
    df = df.withColumn("streak_reset", when((col("is_good") == 1) & ((col("prev_is_good") == 0)|((col("prev_is_good")).isNull())), 1).otherwise(0))
    # Cumulative sum of streak resets to create a unique streak ID
    df = df.withColumn("streak_reset_cumulative", sum(col("streak_reset")).over(windowSpec))
    # Define another window spec including streak_id to calculate streak
    streakWindowSpec = Window.partitionBy(streak_of, "streak_reset_cumulative").orderBy("timestamp")
    # Calculate the streak
    df = df.withColumn("streak", when(col("is_good") == 1, sum(col("is_good")).over(streakWindowSpec)).otherwise(0))
    # Drop unnecessary columns
    df = df.select(col("id"), col("timestamp"), col("date"), col("latitude")
                   , col("longitude"), col("country_name"), col("region"), col("sub-region")
                   , col("sensor_id"), col("P1"), col("P2"), col("aqi"), col("range"), col("clustered_region"), col("optimal_clustered_region")
                   , col("is_good"), col("prev_is_good"),col("streak_reset"), col("streak_reset_cumulative"),col("streak")
                   )
    df.show(10)
    return df


def calculate_max_streak(df, streak_of):
    # Group by sensor_id and find the maximum streak
    df = df.groupBy(streak_of).agg(max("streak").alias("max_streak"))
    # Display the resulting DataFrame
    df.show(10)
    return df

