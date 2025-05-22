import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from src.config import OUTPUT_PATH_PLOTS


import matplotlib.pyplot as plt

def plot_elbow_curve(wcss, optimal_k, path = OUTPUT_PATH_PLOTS):
    plt.figure(figsize=(8, 6))
    plt.plot(range(1, len(wcss) + 1), wcss, marker="o", label="WCSS")
    plt.axvline(x=optimal_k, color="r", linestyle="--", label=f"Optimal k = {optimal_k}")
    plt.xlabel("Number of Clusters (k)")
    plt.ylabel("WCSS")
    plt.title("Elbow Method for Optimal k")
    plt.legend()
    plt.grid()
    plt.show()
    plt.savefig(f"{path}/task_2_elbow_curve.png", bbox_inches='tight')
    print(f"Plot task_2_plot_map saved")

def plot_map(df, plot_name, pk = "clustered_region", show_legend = False, use_scatter = False,output_path = OUTPUT_PATH_PLOTS):
    file_name = plot_name.replace(" ", "_")
    plot_name = plot_name.title()
    print(f"Ploting {plot_name} on World Map")
    df = df.select("latitude", "longitude", pk).toPandas()

    # Convert PySpark DataFrame to GeoDataFrame for plotting
    # Create a 'geometry' column with Points based on latitude and longitude
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    geo_df = gpd.GeoDataFrame(df, geometry=geometry)

    # Grab low resolution world file from NACIS
    # url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    input_path = "data/map/ne_110m_admin_0_countries.zip"
    world = gpd.read_file(input_path)[['SOV_A3', 'POP_EST', 'CONTINENT', 'NAME', 'GDP_MD', 'geometry']]
    world = world.set_index("SOV_A3")

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot world map as the base layer
    world.plot(ax=ax, color='lightgrey', edgecolor='black')
    if use_scatter:
        scatter = ax.scatter(
            geo_df['longitude'], 
            geo_df['latitude'], 
            # c='lightblue', 
            # cmap="viridis", 
            s=30, 
            alpha=0.6, 
            edgecolor="black", 
            label=None
        )
    else:
    # # Plot each cluster with unique colors
        for cluster_id in sorted(geo_df[pk].unique()):
            subset = geo_df[geo_df[pk] == cluster_id]
            subset.plot(ax=ax, markersize=30, label=f'Cluster {cluster_id}', alpha=0.6, edgecolors='k')
    
    # Customize plot
    plt.title(f"{plot_name} on World Map")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    if show_legend:
        plt.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Clusters")
    plt.show()
    plt.savefig(f"{output_path}/{file_name}_map.png", bbox_inches='tight')
    print(f"Plot {file_name}_map saved")

def plot_histogram(df,path = OUTPUT_PATH_PLOTS):
    fig, ax = plt.subplots(figsize=(10, 6))
    # Plot the histogram
    df = df.toPandas()
    # n, bins, patches = ax.hist(df["max_streak"], bins = 20, edgecolor='black')
    n, bins, patches = ax.hist(df["max_streak"], bins = df["max_streak"].max() +1, edgecolor='black')

    for patch in range(len(patches)):
        patch_height = patches[patch].get_height()
        if patch_height > 0:
          ax.text(patches[patch].get_x() + (patches[patch].get_width() / 2), patch_height, f'{int(patch_height)}', ha='center', va='bottom')

    ax.set_xlabel("Max Streak")
    ax.set_ylabel("Frequency")
    ax.set_title("Distribution of Max Streaks")
    plt.show()
    plt.savefig(f"{path}/task_3_max_streaks_histogram.png", bbox_inches='tight')
    print(f"Plot max_streaks_histogram saved")


def histogram_to_table(df):
    # Convert PySpark DataFrame to Pandas DataFrame
    df = df.toPandas()

    # Calculate histogram bins and frequencies
    # n, bins, _ = plt.hist(df["max_streak"], bins= 20)
    n, bins, _ = plt.hist(df["max_streak"], bins= df["max_streak"].max() +1)

    # Create bin range labels
    bin_ranges = [f"{int(bins[i])} - {int(bins[i+1])}" for i in range(len(bins) - 1)]

    # Create a DataFrame to represent the histogram as a table
    histogram_table = pd.DataFrame({
        "Bin Range": bin_ranges,
        "Frequency": n.astype(int)
    })
    return histogram_table
