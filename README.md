# 🌊 Nazaré Big-Wave Risk & Forecasting Dashboard
**Real-time Marine Data • Snowflake • StormGlass API • Streamlit • Forecasting**

## 🌊 Why Nazaré Matters

Nazaré’s giant waves—powered by the deep **Nazaré Canyon**—can suddenly surge above **20–30 meters**, creating some of the most dangerous marine conditions in the world. These waves threaten:

- **Surfers**
- **Rescue teams**
- **Fishermen**
- **Tourists along the coastline**

This dashboard helps improve safety by providing:

- **Real-time wave monitoring**
- **Danger alerts**
- **Environmental insights** (wind, swell, water temperature)
- **24-hour wave forecasting**

Data-driven awareness in a location as extreme as Nazaré can help **protect lives** and support rapid decision-making.

**Nazaré's waves reaching 20–30 meters pose significant risks to surfers, fishermen, and rescue teams.
This dashboard provides live monitoring, pattern insights, hazard detection, and 24-hour forecasting.**

___

This project implements a full big data pipeline using:

**StormGlass Marine Weather API**

**Snowflake (Snowpark + SQL + Streamlit)**

**Python ingestion scripts**

**Machine learning forecasting**

The system monitors real-time marine conditions at **Nazaré, Portugal**, one of the most dangerous big-wave zones on the planet.

## 📍 Coordinates Monitored:

**Latitude: 39.60475**

**Longitude: -9.085443**


# ⚙️ Setup Instructions

## 1️⃣ Clone the repo
```git clone <repo-url>
cd <repo-folder>
```

## 2️⃣ Create .env

```
STORMGLASS_API_KEY=your_api_key
SNOW_ACCOUNT=xxxxxx-xx
SNOW_USER=username
SNOW_PASSWORD=password
SNOW_ROLE=SYSADMIN
SNOW_WAREHOUSE=COMPUTE_WH
SNOW_DATABASE=MARINE_DB
SNOW_SCHEMA=NAZARE_SCHEMA
```

## 3️⃣ Install dependencies
```
pip install -r requirements.txt
```

## 4️⃣ Run the ingestion job
```
python ingest_marine.py
```

## 5️⃣ Run dashboard locally
```
streamlit run nazare-wave-dashboard.py
```


## 🔍 Notebook Analysis 


This notebook uses Snowpark to:

**✔ Load table from Snowflake**

Inspect schema

Validate datatypes

Count rows and timestamp range

**✔ Explore dataset (EDA)**

Wave height distributions

Time-series patterns across days

Correlations (wind ↔ wave, swell ↔ wave)

Daily summaries

**✔ Prepare ML-ready series**

Hourly resampling

Missing value interpolation

Normalized time index


## 🧪 SQL Analytical Queries

**Query #1 — Daily Wave Summary**

```sql
SELECT
    TO_DATE(TIMESTAMP) AS DAY,
    AVG(WAVE_HEIGHT) AS AVG_WAVE_HEIGHT,
    MAX(WAVE_HEIGHT) AS MAX_WAVE_HEIGHT,
    AVG(WIND_SPEED) AS AVG_WIND_SPEED,
    AVG(SWELL_HEIGHT) AS AVG_SWELL_HEIGHT
FROM STORM_MARINE_CLEAN
GROUP BY DAY
ORDER BY DAY DESC;
```

![SQL Query 1](screenshots/SQL_Query_1.png)

**Query #2 — Dangerous Wave Detection**

```sql
SELECT
    COUNT(*) AS DANGEROUS_WAVE_COUNT,
    MIN(WAVE_HEIGHT) AS MIN_DANGER,
    MAX(WAVE_HEIGHT) AS MAX_DANGER,
    AVG(WAVE_HEIGHT) AS AVG_DANGER,
    TO_DATE(TIMESTAMP) AS DAY
FROM STORM_MARINE_CLEAN
WHERE WAVE_HEIGHT > 6.0
GROUP BY DAY
ORDER BY DAY DESC;
```

![SQL Query 2](screenshots/SQL_Query_2.png)

**Query #3 — Correlation Analysis**

```sql
SELECT
    CORR(WAVE_HEIGHT, WIND_SPEED) AS CORR_WAVE_WIND,
    CORR(WAVE_HEIGHT, SWELL_HEIGHT) AS CORR_WAVE_SWELL
FROM STORM_MARINE_CLEAN;
```

![SQL Query 3](screenshots/SQL_Query_3.png)

## 📊 Dashboard Features (Streamlit in Snowflake)

The dashboard includes:

- **Date Selector** – choose which day to analyse  
- **Time Selector** – focus on all times or a specific hour  
- **Danger Threshold Slider** – define what “dangerous” waves mean (e.g. > 6m)  
- **Summary KPIs** – average / max wave height, wind, swell, dangerous wave count  
- **Interactive Wave Map** – location-based view of conditions at Nazaré  
- **Wave Height Gauge Meter** – visual indicator of max wave vs danger threshold  
- **Time-Series Charts** – wave, swell, wind, and temperature over time  
- **Scatter Insights** – relationships between wind/swell and wave height  
- **Hourly Pattern Chart** – average wave height by hour of the day  
- **24-Hour Forecast Model (Machine Learning)** – predicts upcoming wave heights  

---

## 🤖 Machine Learning Forecasting Component

The forecasting component uses **NumPy linear regression (`polyfit`)** to estimate the next 24 hours of wave height.

**Model components:**

- **Input:** last 3 days of hourly wave heights  
- **Output:** predicted wave height for the next 24 hours  
- **Confidence Interval:** approximate 95% band around the forecast  
- **Display:** combined history + forecast chart, with the forecast clearly highlighted


## 📍 Dashboard Overview

![Dashboard Overview](screenshots/Dashboard%201.png)

## 🌊 Wave Map & Gauge Meter

![Wave Map](screenshots/Wave%20Map.png)
![Gauge Meter](screenshots/Gauge%20Meter.png)

## 📈 Forecast Chart

![Forecast Chart](screenshots/Forecast%20Chart.png)

## 📉 Time Series

![Time Series](screenshots/Time%20Series.png)

## 🧪 Notebook Insights

![Wave height time series](screenshots/newplot.png) 
![Wave height distribution](screenshots/Distribution_of_wave_height.png)
![Wind vs wave relationship](screenshots/windspeedvswaveheight.png)
![Daily average and maximum wave height](screenshots/dailyavgandmaxwavaeheight.png)



---

## 🎯 Conclusion

This project demonstrates how combining API data, Snowflake, and machine learning can deliver:

- A **live marine data pipeline**
- An interactive **Streamlit dashboard**
- **Analytical SQL insights**
- A simple but effective **forecasting model**

The result is a practical tool that enhances understanding of Nazaré’s hazardous conditions and showcases the power of modern cloud analytics.

## 🙏 Acknowledgments

This project was made possible with:

- **StormGlass.io** for real-time marine weather data  
- **Snowflake** for data storage, Snowpark processing, and Streamlit deployment  

