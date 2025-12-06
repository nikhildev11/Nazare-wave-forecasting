import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date
from snowflake.snowpark.context import get_active_session

# Get Snowflake session
session = get_active_session()

# ---- CONFIG ----
DEFAULT_LAT = 39.60475
DEFAULT_LON = -9.085443

st.set_page_config(
    page_title="Nazaré Marine Analytics (StormGlass + Snowflake)",
    layout="wide",
)

st.title("🌊 Nazaré Marine Analytics Dashboard")
st.caption("Data source: StormGlass.io · Storage & Compute: Snowflake · UI: Streamlit in Snowflake")

# ---------------------------
# Helper functions
# ---------------------------

@st.cache_data
def get_min_max_date():
    """Get min and max dates present in STORM_MARINE_CLEAN."""
    df = session.sql(
        "SELECT MIN(TO_DATE(TIMESTAMP)) AS MIN_D, "
        "       MAX(TO_DATE(TIMESTAMP)) AS MAX_D "
        "FROM STORM_MARINE_CLEAN"
    ).to_pandas()
    if df.empty or df["MIN_D"][0] is None:
        return None, None
    min_d = pd.to_datetime(df["MIN_D"][0]).date()
    max_d = pd.to_datetime(df["MAX_D"][0]).date()
    return min_d, max_d


@st.cache_data(ttl=60)
def load_data_for_date(day: date) -> pd.DataFrame:
    """Load all rows for a specific date from STORM_MARINE_CLEAN."""
    day_str = day.strftime("%Y-%m-%d")
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
        WHERE TO_DATE(TIMESTAMP) = '{day_str}'
        ORDER BY TIMESTAMP
    """
    return session.sql(query).to_pandas()


# ---------------------------
# Sidebar: filters
# ---------------------------

with st.sidebar:
    st.header("Filters")

    min_d, max_d = get_min_max_date()
    if not min_d or not max_d:
        st.error("No data available in STORM_MARINE_CLEAN. Please run ingestion first.")
        st.stop()

    selected_date = st.date_input(
        "Select date",
        value=max_d,
        min_value=min_d,
        max_value=max_d,
    )

    danger_threshold = st.number_input(
        "Dangerous wave threshold (m)",
        min_value=1.0,
        max_value=25.0,
        value=6.0,
        step=0.5,
    )

# Load data for selected date
df = load_data_for_date(selected_date)

if df.empty:
    st.warning(
        f"No data found in STORM_MARINE_CLEAN for {selected_date}. "
        "Run the ingestion notebook/script or choose another date."
    )
    st.stop()

# Normalize columns
df["timestamp"] = pd.to_datetime(df["TIMESTAMP"])
df["wave_height"] = df["WAVE_HEIGHT"]
df["swell_height"] = df["SWELL_HEIGHT"]
df["wind_speed"] = df["WIND_SPEED"]
df["water_temperature"] = df["WATER_TEMPERATURE"]

# Ensure numeric lat/lon (or fallback to default location)
if "LAT" in df.columns and "LON" in df.columns:
    try:
        df["lat"] = df["LAT"].astype(float)
        df["lon"] = df["LON"].astype(float)
    except Exception:
        df["lat"] = DEFAULT_LAT
        df["lon"] = DEFAULT_LON
else:
    df["lat"] = DEFAULT_LAT
    df["lon"] = DEFAULT_LON

# Build time dropdown from available timestamps (HH:MM)
time_strings = sorted(df["timestamp"].dt.strftime("%H:%M").unique())
time_options = ["All times"] + list(time_strings)

with st.sidebar:
    selected_time = st.selectbox("Select time", options=time_options, index=0)
    st.caption(
        "- **All times** → uses all records for that day\n"
        "- Specific time → focuses on that timestamp in the map & KPIs"
    )

# Apply time filter
if selected_time != "All times":
    df_filtered = df[df["timestamp"].dt.strftime("%H:%M") == selected_time].copy()
else:
    df_filtered = df.copy()

if df_filtered.empty:
    st.warning(f"No data for time {selected_time} on {selected_date}. Showing full day instead.")
    df_filtered = df.copy()

# ---------------------------
# Summary KPIs (cleaner header & spacing)
# ---------------------------

if selected_time == "All times":
    subtitle = selected_date.strftime("%Y-%m-%d")
else:
    subtitle = f"{selected_date.strftime('%Y-%m-%d')} · {selected_time}"

st.subheader(f"Summary · {subtitle}")

c1, c2, c3 = st.columns(3)
c4, c5 = st.columns(2)

avg_wave = df_filtered["wave_height"].mean()
max_wave = df_filtered["wave_height"].max()
avg_wind = df_filtered["wind_speed"].mean()
avg_swell = df_filtered["swell_height"].mean()
danger_records = int((df_filtered["wave_height"] > danger_threshold).sum())

c1.metric("🌊 Avg Wave (m)", f"{avg_wave:.2f}")
c2.metric("🌊 Max Wave (m)", f"{max_wave:.2f}")
c3.metric("🌬️ Avg Wind (m/s)", f"{avg_wind:.2f}")
c4.metric("🌊 Avg Swell (m)", f"{avg_swell:.2f}")
c5.metric("⚠️ Dangerous Records", danger_records)

st.markdown("")  # small spacing
st.markdown("---")

# ---------------------------
# Dangerous wave map (visually clearer)
# ---------------------------

st.subheader("📍 Wave Map (Colored by Wave Height)")

df_map = df_filtered.copy()
df_map["danger"] = df_map["wave_height"] > danger_threshold
df_map["danger_label"] = df_map["danger"].map({True: "Dangerous", False: "Safe"})

# If you have many points at the same location you’ll see one bubble,
# but color & size show how big the waves are.
fig_map = px.scatter_mapbox(
    df_map,
    lat="lat",
    lon="lon",
    color="wave_height",           # continuous color by wave height
    size="wave_height",            # bubble size by wave height
    hover_name="timestamp",
    hover_data={
        "wave_height": True,
        "swell_height": True,
        "wind_speed": True,
        "water_temperature": True,
        "lat": False,
        "lon": False,
    },
    zoom=9,
    height=400,
)

fig_map.update_layout(
    mapbox_style="open-street-map",
    mapbox_center={"lat": float(df_map["lat"].iloc[0]),
                   "lon": float(df_map["lon"].iloc[0])},
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    coloraxis_colorbar_title="Wave (m)",
)

st.plotly_chart(fig_map, use_container_width=True)

st.markdown("---")

# ---------------------------
# Time series (full day for context)
# ---------------------------

st.subheader("📈 Time Series for Selected Date")

df_day = df.sort_values("timestamp").copy()

tab1, tab2, tab3, tab4 = st.tabs(
    ["Wave Height", "Swell Height", "Wind Speed", "Water Temperature"]
)

with tab1:
    fig_wave = px.line(
        df_day,
        x="timestamp",
        y="wave_height",
        title="Wave Height Over Time (m)",
        labels={"timestamp": "Time", "wave_height": "Wave Height (m)"},
    )
    st.plotly_chart(fig_wave, use_container_width=True)

with tab2:
    fig_swell = px.line(
        df_day,
        x="timestamp",
        y="swell_height",
        title="Swell Height Over Time (m)",
        labels={"timestamp": "Time", "swell_height": "Swell Height (m)"},
    )
    st.plotly_chart(fig_swell, use_container_width=True)

with tab3:
    fig_wind = px.line(
        df_day,
        x="timestamp",
        y="wind_speed",
        title="Wind Speed Over Time (m/s)",
        labels={"timestamp": "Time", "wind_speed": "Wind Speed (m/s)"},
    )
    st.plotly_chart(fig_wind, use_container_width=True)

with tab4:
    fig_temp = px.line(
        df_day,
        x="timestamp",
        y="water_temperature",
        title="Water Temperature Over Time (°C)",
        labels={"timestamp": "Time", "water_temperature": "Water Temperature (°C)"},
    )
    st.plotly_chart(fig_temp, use_container_width=True)

st.markdown("---")

# ---------------------------
# Relationships (scatter, filtered data)
# ---------------------------

st.subheader("🔍 Relationships (Filtered Data)")

col_rel1, col_rel2 = st.columns(2)

with col_rel1:
    st.markdown("**Wind Speed vs Wave Height**")
    fig_scatter = px.scatter(
        df_filtered,
        x="wind_speed",
        y="wave_height",
        title="Wind Speed vs Wave Height",
        labels={"wind_speed": "Wind Speed (m/s)", "wave_height": "Wave Height (m)"},
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_rel2:
    st.markdown("**Swell Height vs Wave Height**")
    fig_scatter2 = px.scatter(
        df_filtered,
        x="swell_height",
        y="wave_height",
        title="Swell vs Wave Height",
        labels={"swell_height": "Swell Height (m)", "wave_height": "Wave Height (m)"},
    )
    st.plotly_chart(fig_scatter2, use_container_width=True)

st.markdown("---")

# ---------------------------
# Hourly pattern (full day)
# ---------------------------

st.subheader("⏰ Hourly Wave Pattern (Selected Date)")

df_day["hour"] = df_day["timestamp"].dt.hour
hourly_avg = df_day.groupby("hour")["wave_height"].mean().reset_index()

fig_hour = px.bar(
    hourly_avg,
    x="hour",
    y="wave_height",
    title="Average Wave Height by Hour of Day",
    labels={"hour": "Hour of Day", "wave_height": "Avg Wave Height (m)"},
)
st.plotly_chart(fig_hour, use_container_width=True)