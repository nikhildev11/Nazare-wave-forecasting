import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta

from snowflake.snowpark.context import get_active_session

# Get current Snowflake session
session = get_active_session()

# ---- CONFIG ----
DEFAULT_LAT = 39.60475
DEFAULT_LON = -9.085443
DEFAULT_DAYS_BACK = 3  # default history window for date range

st.set_page_config(
    page_title="Nazaré Marine Analytics (StormGlass + Snowflake)",
    layout="wide",
)

st.title("🌊 Nazaré Marine Analytics Dashboard")
st.caption("Data source: StormGlass.io · Storage & compute: Snowflake · UI: Streamlit-in-Snowflake")

# ---- SIDEBAR FILTERS ----
with st.sidebar:
    st.header("Filters")

    today = date.today()
    default_start = today - timedelta(days=DEFAULT_DAYS_BACK)

    start_date = st.date_input("Start date", value=default_start)
    end_date = st.date_input("End date", value=today)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    danger_threshold = st.number_input(
        "Dangerous wave threshold (m)", min_value=1.0, max_value=25.0, value=6.0, step=0.5
    )

    st.markdown("---")
    st.markdown("**Location**")
    st.write(f"Lat: `{DEFAULT_LAT}`, Lon: `{DEFAULT_LON}`")

# ---- LOAD DATA FROM SNOWFLAKE ----
@st.cache_data(ttl=60)
def load_data(start: date, end: date) -> pd.DataFrame:
    query = f"""
        SELECT
            TIMESTAMP,
            WAVE_HEIGHT,
            SWELL_HEIGHT,
            WIND_SPEED,
            WATER_TEMPERATURE,
            LAT,
            LON
        FROM STORM_MARINE_CLEAN
        WHERE ::DATE(TIMESTAMP) BETWEEN '{start.strftime("%Y-%m-%d")}'
                                   AND '{end.strftime("%Y-%m-%d")}'
        ORDER BY TIMESTAMP
    """
    df = session.sql(query).to_pandas()
    return df

df = load_data(start_date, end_date)

if df.empty:
    st.warning("No data found in STORM_MARINE_CLEAN for the selected date range. "
               "Run the ingestion pipeline or choose a different period.")
    st.stop()

# Normalize column names in DataFrame for convenience
df["timestamp"] = pd.to_datetime(df["TIMESTAMP"])
df["wave_height"] = df["WAVE_HEIGHT"]
df["swell_height"] = df["SWELL_HEIGHT"]
df["wind_speed"] = df["WIND_SPEED"]
df["water_temperature"] = df["WATER_TEMPERATURE"]

# ---- SUMMARY KPIs ----
st.subheader(f"Summary · {start_date.isoformat()} → {end_date.isoformat()}")

avg_wave = df["wave_height"].mean()
max_wave = df["wave_height"].max()
avg_wind = df["wind_speed"].mean()
avg_swell = df["swell_height"].mean()
danger_hours = (df["wave_height"] > danger_threshold).sum()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("🌊 Avg Wave Height", f"{avg_wave:.2f} m")
c2.metric("🌊 Max Wave Height", f"{max_wave:.2f} m")
c3.metric("🌬️ Avg Wind Speed", f"{avg_wind:.2f} m/s")
c4.metric("🌊 Avg Swell Height", f"{avg_swell:.2f} m")
c5.metric("⚠️ Dangerous Hours", int(danger_hours))

st.markdown("---")

# ---- MAP ----
st.subheader("📍 Location Map")

# If table has LAT/LON, use median; otherwise default
lat = df["LAT"].median() if "LAT" in df.columns else DEFAULT_LAT
lon = df["LON"].median() if "LON" in df.columns else DEFAULT_LON

map_df = pd.DataFrame({"lat": [lat], "lon": [lon]})
st.map(map_df, zoom=10)

st.markdown("---")

# ---- TIME SERIES VISUALS ----
st.subheader("📈 Time Series Visualizations")

tab1, tab2, tab3, tab4 = st.tabs(
    ["Wave Height", "Swell Height", "Wind Speed", "Water Temperature"]
)

with tab1:
    fig_wave = px.line(
        df,
        x="timestamp",
        y="wave_height",
        title="Wave Height Over Time (m)",
        labels={"timestamp": "Time", "wave_height": "Wave Height (m)"},
    )
    st.plotly_chart(fig_wave, use_container_width=True)

with tab2:
    fig_swell = px.line(
        df,
        x="timestamp",
        y="swell_height",
        title="Swell Height Over Time (m)",
        labels={"timestamp": "Time", "swell_height": "Swell Height (m)"},
    )
    st.plotly_chart(fig_swell, use_container_width=True)

with tab3:
    fig_wind = px.line(
        df,
        x="timestamp",
        y="wind_speed",
        title="Wind Speed Over Time (m/s)",
        labels={"timestamp": "Time", "wind_speed": "Wind Speed (m/s)"},
    )
    st.plotly_chart(fig_wind, use_container_width=True)

with tab4:
    fig_temp = px.line(
        df,
        x="timestamp",
        y="water_temperature",
        title="Water Temperature Over Time (°C)",
        labels={"timestamp": "Time", "water_temperature": "Water Temperature (°C)"},
    )
    st.plotly_chart(fig_temp, use_container_width=True)

st.markdown("---")

# ---- RELATIONSHIPS / SCATTER ----
st.subheader("🔍 Relationships")

col_rel1, col_rel2 = st.columns(2)

with col_rel1:
    st.markdown("**Wind Speed vs Wave Height**")
    fig_scatter = px.scatter(
        df,
        x="wind_speed",
        y="wave_height",
        trendline="ols",
        labels={"wind_speed": "Wind Speed (m/s)", "wave_height": "Wave Height (m)"},
        title="Wind Speed vs Wave Height",
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_rel2:
    st.markdown("**Swell Height vs Wave Height**")
    fig_scatter2 = px.scatter(
        df,
        x="swell_height",
        y="wave_height",
        trendline="ols",
        labels={"swell_height": "Swell Height (m)", "wave_height": "Wave Height (m)"},
        title="Swell vs Wave Height",
    )
    st.plotly_chart(fig_scatter2, use_container_width=True)

st.markdown("---")

# ---- HOURLY PATTERN ----
st.subheader("⏰ Hourly Wave Pattern")

df["hour"] = df["timestamp"].dt.hour
hourly_avg = df.groupby("hour")["wave_height"].mean().reset_index()

fig_hour = px.bar(
    hourly_avg,
    x="hour",
    y="wave_height",
    title="Average Wave Height by Hour of Day",
    labels={"hour": "Hour of Day", "wave_height": "Avg Wave Height (m)"},
)
st.plotly_chart(fig_hour, use_container_width=True)

# ---- RAW DATA ----
st.subheader("📄 Raw Data (Preview)")

st.dataframe(
    df[
        [
            "timestamp",
            "wave_height",
            "swell_height",
            "wind_speed",
            "water_temperature",
        ]
    ].tail(200),
    use_container_width=True,
)