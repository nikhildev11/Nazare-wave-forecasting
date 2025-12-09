import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
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


@st.cache_data(ttl=300)
def load_recent_history(days: int = 3) -> pd.DataFrame:
    """Load recent history (last N days) for forecasting."""
    query = f"""
        WITH MAX_TS AS (
          SELECT MAX(TIMESTAMP) AS MAX_T FROM STORM_MARINE_CLEAN
        )
        SELECT c.TIMESTAMP, c.WAVE_HEIGHT
        FROM STORM_MARINE_CLEAN c, MAX_TS m
        WHERE c.TIMESTAMP >= DATEADD('day', -{days}, m.MAX_T)
        ORDER BY c.TIMESTAMP
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
# Summary KPIs (clean header & spacing)
# ---------------------------

if selected_time == "All times":
    subtitle = selected_date.strftime("%Y-%m-%d")
else:
    subtitle = f"{selected_date.strftime('%Y-%m-%d')} · {selected_time}"

st.subheader(f"Summary · {subtitle}")
st.markdown("")

row1 = st.columns(4)
row2 = st.columns(2)

avg_wave = df_filtered["wave_height"].mean()
max_wave = df_filtered["wave_height"].max()
avg_wind = df_filtered["wind_speed"].mean()
avg_swell = df_filtered["swell_height"].mean()
danger_records = int((df_filtered["wave_height"] > danger_threshold).sum())

row1[0].metric("Avg Wave (m)", f"{avg_wave:.2f}")
row1[1].metric("Max Wave (m)", f"{max_wave:.2f}")
row1[2].metric("Avg Wind (m/s)", f"{avg_wind:.2f}")
row1[3].metric("Avg Swell (m)", f"{avg_swell:.2f}")

row2[0].metric("Dangerous Records", danger_records)
row2[1].markdown(f"**Danger threshold:** {danger_threshold:.1f} m")

st.markdown("---")

# ---------------------------
# Map + Wave Height Meter (side by side)
# ---------------------------

st.subheader("📍 Wave Map & Wave Height Meter")

map_col, gauge_col = st.columns([2, 1])

# ---- Simple map in left column (st.map) ----
with map_col:
    df_map = df_filtered.copy()
    df_map["danger"] = df_map["wave_height"] > danger_threshold

    # st.map expects columns named lat / lon
    map_points = df_map[["lat", "lon"]].dropna()

    if not map_points.empty:
        st.map(map_points, zoom=10, use_container_width=True)
    else:
        st.info("No latitude/longitude available to plot on the map.")

    num_safe = int((~df_map["danger"]).sum())
    num_danger = int(df_map["danger"].sum())
    st.caption(
        f"Safe records: {num_safe} · Dangerous records (> {danger_threshold:.1f} m): {num_danger}"
    )

# ---- Wave height meter in right column ----
with gauge_col:
    st.markdown("**Daily Wave Height Meter**")

    gauge_max = max(max_wave, danger_threshold) * 1.3 if max_wave > 0 else danger_threshold * 1.5

    fig_gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=max_wave,
            title={"text": "Max Wave (m)"},
            gauge={
                "axis": {"range": [0, gauge_max]},
                "steps": [
                    {"range": [0, danger_threshold], "color": "#b7e4c7"},     # safe
                    {"range": [danger_threshold, gauge_max], "color": "#ffccd5"},  # danger
                ],
                "threshold": {
                    "line": {"color": "red", "width": 4},
                    "thickness": 0.8,
                    "value": danger_threshold,
                },
            },
        )
    )

    fig_gauge.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig_gauge, use_container_width=True)
    st.caption("Green = below danger threshold. Red = high-risk wave heights.")

st.markdown("---")

# ---------------------------
# Forecasting Section
# ---------------------------

st.subheader("🔮 Wave Height Forecast (Next 24 Hours)")

hist_df = load_recent_history(days=3)

if hist_df.empty:
    st.info("Not enough history in STORM_MARINE_CLEAN to build a forecast yet.")
else:
    hist_df["timestamp"] = pd.to_datetime(hist_df["TIMESTAMP"])
    hist_df["wave_height"] = hist_df["WAVE_HEIGHT"]

    # Resample to hourly and fill gaps
    ts = (
        hist_df.set_index("timestamp")["wave_height"]
        .resample("H")
        .mean()
        .interpolate()
    )

    if len(ts) < 10:
        st.info("Need more than 10 hourly points to forecast. Ingest more data.")
    else:
        # Simple linear trend forecast using numpy.polyfit
        base_time = ts.index[0]
        t_hist = (ts.index - base_time).total_seconds() / 3600.0  # hours since start
        y_hist = ts.values

        coeffs = np.polyfit(t_hist, y_hist, deg=1)  # linear trend
        trend_hist = np.polyval(coeffs, t_hist)
        residuals = y_hist - trend_hist
        sigma = np.std(residuals)

        # Forecast next 24 hours – start exactly at last history point
        horizon_hours = 24
        last_t = t_hist[-1]
        future_t = np.arange(last_t, last_t + horizon_hours + 1)  # includes last_t
        future_index = [
            ts.index[-1] + pd.Timedelta(hours=i)
            for i in range(0, horizon_hours + 1)
        ]
        forecast_values = np.polyval(coeffs, future_t)

        upper = forecast_values + 1.96 * sigma
        lower = np.clip(forecast_values - 1.96 * sigma, 0, None)

        fig_forecast = go.Figure()

        # History
        fig_forecast.add_trace(
            go.Scatter(
                x=ts.index,
                y=ts.values,
                mode="lines",
                name="History",
                line=dict(color="#1f77b4"),
            )
        )

        # Forecast line
        fig_forecast.add_trace(
            go.Scatter(
                x=future_index,
                y=forecast_values,
                mode="lines",
                name="Forecast",
                line=dict(color="#ff7f0e", dash="solid"),
            )
        )

        # Confidence band (forecast region only)
        fig_forecast.add_trace(
            go.Scatter(
                x=future_index + future_index[::-1],
                y=list(upper) + list(lower[::-1]),
                fill="toself",
                name="95% CI (Forecast)",
                opacity=0.2,
                line=dict(width=0),
                showlegend=True,
            )
        )

        fig_forecast.update_layout(
            title="Wave Height Forecast (Next 24 Hours)",
            xaxis_title="Time (UTC)",
            yaxis_title="Wave Height (m)",
            margin=dict(l=10, r=10, t=40, b=10),
        )

        st.plotly_chart(fig_forecast, use_container_width=True)
        st.caption(
            "History (blue) and 24-hour linear trend forecast (orange). "
            "Shaded area shows an approximate 95% confidence band for the forecast region."
        )

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
