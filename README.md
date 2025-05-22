# Air Quality Analysis Using PySpark & Docker

This project analyzes global air quality data collected by Sensor.Community using Big Data tools. We focused on identifying regions with improving air quality, clustering sensors geographically, and detecting long streaks of clean air using scalable PySpark pipelines.

---

## 🌍 Objective

- Track **daily AQI (Air Quality Index)** trends worldwide
- Detect regions with significant **24-hour improvement**
- Identify **long streaks of good air quality** using window functions
- Cluster sensors into meaningful **geographic groups** using K-means

---

## 🧰 Tools & Stack

- **PySpark**: Data processing & clustering
- **Docker**: Portable & reproducible environment
- **Matplotlib, GeoPandas, Shapely**: Visualization
- **KneeLocator**: Finding optimal number of clusters
- **Sensor.Community API**: Live air quality sensor data (PM2.5 & PM10)

---

## 🔄 Workflow

1. **Data Collection**: Daily API pulls (Oct 31–Nov 10, 2024)
2. **Preprocessing**:
   - JSON flattening & schema enforcement
   - Null filtering, deduplication, geospatial cleanup
3. **AQI Calculation**: Based on UK DEFRA standards
4. **Analysis Tasks**:
   - Task 1: Top 10 countries with 24h AQI improvement
   - Task 2: Top 50 improving regions (200 K-means clusters)
   - Task 3: Longest streaks of clean air (AQI ≤ 3)

---

## 📊 Results Summary

### Top 10 Countries with AQI Improvement (Nov 10 vs 9)

| Rank | Country         | Prev AQI | Current AQI | Δ AQI |
|------|------------------|----------|--------------|--------|
| 1    | South Africa     | 1.00     | 3.00         | +2.00  |
| 2    | Uzbekistan       | 2.67     | 4.33         | +1.66  |
| 3    | Denmark          | 2.80     | 4.10         | +1.30  |
| ...  | ...              | ...      | ...          | ...    |

> 📌 60% of the most improved **regions** were in Eastern & Western Europe

---

## 📍 Clustering & Mapping

- **K = 200 clusters** used to define regional boundaries
- **Elbow method** applied → optimal K ≈ 4, but higher K used for granularity
- Clusters mapped using GeoPandas for visual exploration

---

## 📈 Longest Streaks of Clean Air

- Defined as consecutive days where AQI ≤ 3
- Streaks tracked per sensor over 11-day window
- Histogram shows **many sensors had 11-day streaks**
- Clusters with longest streaks: **Eastern Europe**, **West Asia**, and **SE Asia**

---
##  🐳 Docker-Based Execution

1. **Docker**: [Install Docker](https://www.docker.com/get-started)
2. Have Docker running

## Quick Start

### Step 1: Open terminal/cmd and go to the folder pyspark_aqi_assignment

- **Linux/macOS**:
  ```bash
  cd ~/path/pyspark_aqi_assignment

- **Windows**:
  ```CMD
  cd C:\Users\YourUsername\path\pyspark_aqi_assignment

### Step 2: Run the Application

Simply run the provided shell or batch script to build and execute the Docker container:

- **Linux/macOS**:
  ```bash
  ./run_docker_unix.sh

- **Windows**:
  ```CMD
  run_docker_win.bat

---
Container executes:

- Data extraction
- Preprocessing & AQI calculation
- Spark clustering & visualizations
- Output results and plots in `/results`

---

## 📝 Local Performance

Tested PySpark in `local[n]` mode:

| Threads | Time (mm:ss) | Optimal K |
|---------|---------------|-----------|
| 1       | 03:41         | 5         |
| 4       | 02:40         | 5         |
| 6       | 02:38         | 4         |

---

## 💡 Key Insights
- Data preprocessing cut Spark load time from 30+ mins → seconds
-	Clustering on geolocation gave better insight than country-level analysis
-	Sensor density imbalances (Europe vs Africa) biased some results
	•	Dockerized Spark setup made the analysis portable & reproducible
