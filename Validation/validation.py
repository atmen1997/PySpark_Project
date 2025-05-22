'''
Created on 23 Nov 2024

@author: Cristo
'''
import pandas as pd
from sklearn.cluster import DBSCAN,KMeans
import matplotlib.pyplot as plt
from pickle import NONE
from _overlapped import NULL
import os
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import folium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
#from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
import time
from webdriver_manager.chrome import ChromeDriverManager
import matplotlib
from geopy.geocoders import Nominatim

matplotlib.use('Agg')
geolocator = Nominatim(user_agent="geoapi")
pd.set_option('display.width', 100000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)
pd.set_option('max_colwidth', 1000)


def type_value(sensor_data, value_type):
    for item in sensor_data:
        if item['value_type'] == value_type:
            return float(item['value'])
    return None


def calculateAQI(pm10, pm25):
    aqi = 0
    x = 0
    y = 0
    # print("pm25=",pm25)
    # print("pm10=",pm10)
    if pm25 == NONE or pm25 == NULL or pm25 == '':
        pm25 = 0
    if pm10 == NONE or pm10 == NULL or pm10 == '':
        pm10 = 0
    pm10 = float(pm10)
    pm25 = float(pm25)
    if 0 < pm25 <= 11:
        x = 1 
    elif 11 < pm25 <= 23:
        x = 2
    elif 23 < pm25 <= 35:
        x = 3
    elif 35 < pm25 <= 41:
        x = 4
    elif 41 < pm25 <= 47:
        x = 5
    elif 47 < pm25 <= 53:
        x = 6
    elif 53 < pm25 <= 58:
        x = 7
    elif 58 < pm25 <= 64:
        x = 8
    elif 64 < pm25 <= 70:
        x = 9
    elif pm25 > 70:
        x = 10
    else:
        x = 0
            
    if 0 < pm10 <= 16:
        y = 1
    elif 16 < pm10 <= 33:
        y = 2
    elif 33 < pm10 <= 50:
        y = 3
    elif 50 < pm10 <= 58:
        y = 4
    elif 58 < pm10 <= 66:
        y = 5
    elif 66 < pm10 <= 75:
        y = 6
    elif 75 < pm10 <= 83:
        y = 7
    elif 83 < pm10 <= 91:
        y = 8
    elif 91 < pm10 <= 100:
        y = 9
    elif pm10 > 100:
        y = 10
    else:
        y = 0

    if x == y:
        aqi = x
    elif x > y:
        aqi = x
    else:
        aqi = y
            
    return aqi


def calculateRange(aqi):
    
    range = ""
    if 1 <= aqi <= 3:
        range = "Low"
    elif 4 <= aqi <= 6:
        range = "Medium"
    elif 7 <= aqi <= 9:
        range = "High"
    else:
        range = "Very High"
        
    return range


def getTask1Data(file_timestamps):
    
    processed_data = []
    for file, timestamp in file_timestamps:
        
        data = pd.read_json(os.path.join(file_path, file))
        data = initializeData(data)
        
        country_aqi = data.groupby('country_code')['AQI'].mean().reset_index()
        country_aqi['timestamp'] = timestamp
        # append all data
        processed_data.append(country_aqi)
    return processed_data


    
def initializeData(df):
    df['country_code'] = df['location'].apply(lambda x: x.get('country'))
    df['P10'] = df['sensordatavalues'].apply(lambda x: type_value(x, 'P1'))
    df['P25'] = df['sensordatavalues'].apply(lambda x: type_value(x, 'P2'))
    df['AQI'] = df.apply(lambda row: calculateAQI(row['P10'], row['P25']), axis=1)

    df['Range'] = df['AQI'].apply(calculateRange)
    df['latitude'] = df['location'].apply(lambda x:round(float(x.get('latitude')), 2))
    df['longitude'] = df['location'].apply(lambda x:round(float(x.get('longitude')), 2))
    
    return df




def getCountry_info(df):
    country_info = pd.read_csv("data/country_info_data/country_info.csv")
    country_info = country_info[['alpha-2', 'name', 'region']]
    country_info.rename(columns={'alpha-2': 'country_code', 'name': 'country_name'}, inplace=True)
    country_info = pd.merge(df, country_info, on='country_code', how='left')
    return country_info
    
def plot_map(df, plot_name, pk = "clustered_region",path = ""):
    file_name = plot_name.replace(" ", "_")
    print(f"Ploting {plot_name} on World Map")
    #df = df[['latitude', 'longitude','region']]

    # # Convert PySpark DataFrame to GeoDataFrame for plotting
    # # Create a 'geometry' column with Points based on latitude and longitude
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    geo_df = gpd.GeoDataFrame(df, geometry=geometry)

    # # Grab low resolution world file from NACIS
    url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    world = gpd.read_file(url)[['SOV_A3', 'POP_EST', 'CONTINENT', 'NAME', 'GDP_MD', 'geometry']]
    world = world.set_index("SOV_A3")

    # # Plotting
    fig, ax = plt.subplots(figsize=(12, 8))

    # # Plot world map as the base layer
    world.plot(ax=ax, color='lightgrey', edgecolor='black')

    # # Plot each cluster with unique colors
    for cluster_id in sorted(geo_df[pk].unique()):
        subset = geo_df[geo_df[pk] == cluster_id]
        subset.plot(ax=ax, markersize=30, label=f'Cluster {cluster_id}', alpha=0.6, edgecolors='k')
    # # Customize plot
    plt.title(f"Clustered {plot_name} on World Map")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Clusters")
    plt.show()
    plt.savefig(f"{path}/{file_name}_map.png", bbox_inches='tight')
    print(f"Plot {plot_name}_map saved")
    
    
def foliumMap(data):
    grouped = data.groupby("region").agg(
    latitude_center=("latitude", "mean"),  
    longitude_center=("longitude", "mean"),  
    points=("latitude", lambda x: [[lat, lon] for lat, lon in zip(x, data.loc[x.index, "longitude"])]), 
    AQI_mean=("AQI", "mean"),  
).reset_index()

    points = grouped[['latitude_center', 'longitude_center']].values.tolist()
    clustering = DBSCAN(eps=5, min_samples=2).fit(points)
    labels = clustering.labels_
    
    
    largest_cluster = np.argmax(np.bincount(labels[labels >= 0]))
    for l in labels:
        focused_points = points[(l == largest_cluster).astype(bool)]
        
    m = folium.Map(location=[grouped["latitude_center"].mean(), grouped["longitude_center"].mean()], zoom_start=4)
    
    
    for _, row in grouped.iterrows():
        
        folium.Marker(
            location=[row["latitude_center"], row["longitude_center"]],
            popup=f"Region: {row['region']}<br>Center: ({row['latitude_center']}, {row['longitude_center']})<br>AQI Mean: {row['AQI_mean']}",
            icon=folium.Icon(color="blue", icon="info-sign"),
        ).add_to(m)
        
        
        folium.PolyLine(row["points"], color="red", weight=2.5, opacity=0.8).add_to(m)
    m.fit_bounds(focused_points)
    
    m.save("task2_map.html")
    html_file = os.path.abspath("task2_map.html")
    screenshot_file=os.path.abspath("task2_map_screenshot.png")
    options =webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1024,768')
    driver=webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),options=options)
    driver.get(f'file://{html_file}')
    time.sleep(10)
    driver.save_screenshot(screenshot_file)
    driver.quit()
    image=Image.open(screenshot_file)
    image=image.crop((0,0,1024,768))
    image.save(screenshot_file)
    

def get_location(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), exactly_one=True)
        if location:
            return location.address
        else:
            return "Location not found"
    except Exception as e:
        return str(e)
    
start_time = datetime.now()
print(f"Program start time: {start_time}")

print("loading data...")

file_path = "data/sensor_data/"  
file_pattern = "data_24h_*.json"  
# get all name of JSON files
all_files = [f for f in os.listdir(file_path) if f.startswith("data_24h_") and f.endswith(".json")]
# order by time
file_timestamps = [(f, datetime.strptime(f.split("_")[2] + f.split("_")[3][:4], '%Y%m%d%H%M')) for f in all_files]
file_timestamps.sort(key=lambda x: x[1])  
    
    
    
    
    
print("---------------Executing Task 1: Top 10 countries with AQI improvement...")
task1_processed_data = getTask1Data(file_timestamps)
# Merge all of the processed data 
task1_all_data = pd.concat(task1_processed_data, ignore_index=True)  
# reorder
task1_all_data = task1_all_data.sort_values(by=['country_code', 'timestamp']).reset_index(drop=True)


improvements = []
for country in task1_all_data['country_code'].unique():
    
    country_data = task1_all_data[task1_all_data['country_code'] == country].sort_values('timestamp')
    
    country_data['improvement'] = round(country_data['AQI'].diff(-1),2)
    improvements.append(country_data)

improvements_df = pd.concat(improvements, ignore_index=True)
country_improvements = improvements_df.groupby(['country_code'], as_index=False)['improvement'].mean()
top_countries = country_improvements.sort_values('improvement', ascending=False).head(10)

result_task1 = getCountry_info(top_countries)
order = ['country_name', 'country_code', 'improvement']

result_task1 = result_task1[order]
print(result_task1)


print("---------------Executing Task 2: Top 50 AQI improvements by clustered region...")


task2_processed_data = []

for file, timestamp in file_timestamps:
    
    data = pd.read_json(os.path.join(file_path, file))
    data['country_code'] = data['location'].apply(lambda x: x.get('country'))
    data['latitude'] = round(data['location'].apply(lambda x: float(x.get('latitude'))), 2)
    data['longitude'] = round(data['location'].apply(lambda x: float(x.get('longitude'))), 2)
    data['P10'] = round(data['sensordatavalues'].apply(lambda x: type_value(x, 'P1')),2)
    data['P25'] = round(data['sensordatavalues'].apply(lambda x: type_value(x, 'P2')),2)
    data['AQI'] = round(data.apply(lambda row: calculateAQI(row['P10'], row['P25']), axis=1),2)
    data['timestamp'] = timestamp
    task2_processed_data.append(data)

task2_all_data = pd.concat(task2_processed_data)

coords = task2_all_data[['latitude', 'longitude']].dropna()
kmeans = KMeans(n_clusters=50, random_state=42)
task2_all_data['region'] = kmeans.fit_predict(coords)

#task2_geo_aqi = task2_all_data.groupby(['region', 'timestamp'])['AQI'].mean().reset_index()
task2_geo_aqi = task2_all_data.groupby(['region', 'timestamp']).agg(
    AQI=('AQI', lambda x: round(x.mean(), 2)),  
    latitude=('latitude', lambda x: round(x.mean(), 2)),
    longitude=('longitude', lambda x: round(x.mean(), 2))
).reset_index()


task2_improvements = []
for region in task2_geo_aqi['region'].unique():
    task2_region_data = task2_geo_aqi[task2_geo_aqi['region'] == region].sort_values('timestamp')
    task2_region_data['improvement'] = round(task2_region_data['AQI'].diff(-1),2)
    
    task2_improvements.append(task2_region_data)


task2_improvement_data = pd.concat(task2_improvements)


task2_top_50_improvements = task2_improvement_data.nlargest(50, 'improvement')
task2_top_50_improvements

task2_top_50_improvements['address'] = task2_top_50_improvements.apply(lambda row: get_location(row['latitude'], row['longitude']), axis=1)


#plot_map(task2_top_50_improvements, "region")
foliumMap(task2_top_50_improvements)
print(task2_top_50_improvements)






print("------------------Executing Task 3: Longest streaks of good air quality...")

all_files = [f for f in os.listdir(file_path) if f.startswith(file_pattern)]
all_files.sort()  # Ensure files are ordered by time

processed_data = []

for file, timestamp in file_timestamps:
    # Load data
    data = pd.read_json(os.path.join(file_path, file))
    
    # Process columns
    data['P10'] = data['sensordatavalues'].apply(lambda x: type_value(x, 'P1'))
    data['P25'] = data['sensordatavalues'].apply(lambda x: type_value(x, 'P2'))
    data['AQI'] = data.apply(lambda row: calculateAQI(row['P10'], row['P25']), axis=1)
    data['timestamp'] = datetime.strptime(file.split("_")[2] + file.split("_")[3][:4], "%Y%m%d%H%M")
    data['latitude'] = data['location'].apply(lambda x: round(float(x['latitude']), 2))
    data['longitude'] = data['location'].apply(lambda x: round(float(x['longitude']), 2))
    processed_data.append(data)

# Combine all data
combined_data = pd.concat(processed_data)
combined_data['is_good'] = combined_data['AQI'].between(1, 3)
all_data = combined_data.sort_values(['latitude', 'longitude', 'timestamp'])
grouped = all_data.groupby(['latitude', 'longitude'])
# Group by location (latitude & longitude) and calculate streaks
streaks = []
for (lat, lon), group in grouped:
    group = group.sort_values('timestamp')  # Sort by timestamp
    '''streak = (group['is_good'] != group['is_good'].shift()).cumsum()
    group['streak_length'] = group.groupby(streak)['is_good'].transform('count')
    streaks.append(group)'''
    group['streak'] = group['is_good'].astype(int).groupby((group['is_good'] != group['is_good'].shift()).cumsum()).cumsum()
    longest_streak = group[group['is_good']]['streak'].max() if not group[group['is_good']].empty else 0
    streaks.append(longest_streak)


# Plot histogram
plt.hist(streaks, bins=20, range=(0, max(streaks)), edgecolor='black')
plt.title("Histogram of Good Air Quality Streaks")
plt.xlabel("Streak Length")
plt.ylabel("Frequency")



plt.savefig("task_3_histogram.png", bbox_inches='tight')
plt.show()

end_time = datetime.now()
print(f"Program end time: {end_time}")
print(f"Program run duration: {end_time - start_time}")
