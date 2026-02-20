import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.loader import load_streaming_files, clean_streaming_data
from src.analysis import (
    get_overview_stats,
    get_top_items,
    get_hourly_heatmap_data,
    get_skip_analysis,
    get_platform_breakdown,
    get_yearly_trend,
    analyze_weekend_vs_weekday_listening,
    get_listening_personality,
)

# ── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Spotify Listening Tracker",
    page_icon="🎵",
    layout="wide",
)

# ── Styles ───────────────────────────────────────────────────────────────────

st.markdown("""
    <style>
        .metric-card {
            background-color: #1DB954;
            padding: 1rem;
            border-radius: 10px;
            color: white;
            text-align: center;
        }
        .section-header {
            color: #1DB954;
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 2rem;
        }
    </style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────────────────────

st.title("🎵 Spotify Listening Tracker")
st.markdown("Upload your Spotify extended streaming history to explore your listening habits.")

# ── File Upload ───────────────────────────────────────────────────────────────

st.markdown("### Upload Your Spotify Data")
st.markdown(
    "Download your data from Spotify: **Account → Privacy Settings → Download your data** "
    "(request Extended Streaming History). Upload all your `Streaming_History_Audio_*.json` files below."
)

uploaded_files = st.file_uploader(
    "Upload your Streaming_History_Audio JSON files",
    type="json",
    accept_multiple_files=True,
)

# ── Load and Process ──────────────────────────────────────────────────────────

@st.cache_data
def process_uploaded_files(files) -> pd.DataFrame:
    import json, tempfile, os

    dataframes = []
    for file in files:
        data = json.load(file)
        dataframes.append(pd.DataFrame(data))

    combined = pd.concat(dataframes, ignore_index=True)
    return clean_streaming_data(combined)


if not uploaded_files:
    st.info("Upload your files above to get started. Your data never leaves your browser session.")
    st.stop()

df = process_uploaded_files(uploaded_files)

# ── Year Filter (Sidebar) ─────────────────────────────────────────────────────

st.sidebar.title("🎛️ Filters")
available_years = sorted(df["year"].unique().tolist())
selected_year = st.sidebar.selectbox(
    "Filter by Year",
    options=["All Time"] + available_years,
)

df_filtered = df if selected_year == "All Time" else df[df["year"] == selected_year]
year_param = None if selected_year == "All Time" else selected_year

# ── Section 1: Overview ───────────────────────────────────────────────────────

st.markdown('<p class="section-header">📊 Overview</p>', unsafe_allow_html=True)

stats = get_overview_stats(df_filtered)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Hours Listened", f"{stats['total_hours_listened']:,}")
col2.metric("Total Streams", f"{stats['total_streams']:,}")
col3.metric("Unique Artists", f"{stats['unique_artists']:,}")
col4.metric("Unique Tracks", f"{stats['unique_tracks']:,}")
col5.metric("Unique Albums", f"{stats['unique_albums']:,}")

st.caption(f"Data from {stats['date_range_start']} to {stats['date_range_end']}")

# ── Section 2: Yearly Trend ───────────────────────────────────────────────────

if selected_year == "All Time":
    st.markdown('<p class="section-header">📈 Listening Over the Years</p>', unsafe_allow_html=True)
    yearly = get_yearly_trend(df_filtered)
    fig = px.bar(
        yearly,
        x="year",
        y="total_minutes",
        labels={"total_minutes": "Minutes Listened", "year": "Year"},
        color_discrete_sequence=["#1DB954"],
    )
    fig.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
    st.plotly_chart(fig, use_container_width=True)

# ── Section 3: Top Artists / Tracks / Albums ──────────────────────────────────

st.markdown('<p class="section-header">🏆 Your Top Artists, Tracks & Albums</p>', unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Artists", "Tracks", "Albums"])

with tab1:
    top_artists = get_top_items(df_filtered, "artist", n=10, year=year_param)
    fig = px.bar(
        top_artists.sort_values("total_minutes"),
        x="total_minutes", y="artist", orientation="h",
        labels={"total_minutes": "Minutes Played", "artist": "Artist"},
        color_discrete_sequence=["#1DB954"],
    )
    fig.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    top_tracks = get_top_items(df_filtered, "track", n=10, year=year_param)
    fig = px.bar(
        top_tracks.sort_values("total_minutes"),
        x="total_minutes", y="track", orientation="h",
        labels={"total_minutes": "Minutes Played", "track": "Track"},
        color_discrete_sequence=["#1DB954"],
    )
    fig.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    top_albums = get_top_items(df_filtered, "album", n=10, year=year_param)
    fig = px.bar(
        top_albums.sort_values("total_minutes"),
        x="total_minutes", y="album", orientation="h",
        labels={"total_minutes": "Minutes Played", "album": "Album"},
        color_discrete_sequence=["#1DB954"],
    )
    fig.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
    st.plotly_chart(fig, use_container_width=True)

# ── Section 4: Listening Heatmap ──────────────────────────────────────────────

st.markdown('<p class="section-header">🗓️ When Do You Listen?</p>', unsafe_allow_html=True)

heatmap_data = get_hourly_heatmap_data(df_filtered)

fig = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=[f"{h}:00" for h in heatmap_data.columns],
    y=heatmap_data.index.tolist(),
    colorscale="Greens",
    showscale=True,
))
fig.update_layout(
    xaxis_title="Hour of Day",
    yaxis_title="Day of Week",
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="white",
)
st.plotly_chart(fig, use_container_width=True)

# ── Section 5: Skip Analysis ──────────────────────────────────────────────────

st.markdown('<p class="section-header">⏭️ What Do You Skip?</p>', unsafe_allow_html=True)

skip_data = get_skip_analysis(df_filtered)

if skip_data.empty:
    st.info("Not enough streams per artist in the selected period to calculate meaningful skip rates.")
else:
    fig = px.bar(
        skip_data.sort_values("skip_rate"),
        x="skip_rate", y="artist", orientation="h",
        labels={"skip_rate": "Skip Rate (%)", "artist": "Artist"},
        color_discrete_sequence=["#e74c3c"],
    )
    fig.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
    st.plotly_chart(fig, use_container_width=True)

# ── Section 6: Platform Breakdown ────────────────────────────────────────────

st.markdown('<p class="section-header">📱 Where Do You Listen?</p>', unsafe_allow_html=True)

platform_data = get_platform_breakdown(df_filtered)
fig = px.pie(
    platform_data,
    names="platform_category",
    values="total_minutes",
    color_discrete_sequence=px.colors.sequential.Greens_r,
)
fig.update_layout(paper_bgcolor="#0e1117", font_color="white")
st.plotly_chart(fig, use_container_width=True)

# ── Section 7: Statistical Insight ───────────────────────────────────────────

st.markdown('<p class="section-header">🔬 Weekend vs Weekday Listening</p>', unsafe_allow_html=True)

hypothesis = analyze_weekend_vs_weekday_listening(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric("Avg Weekday (mins)", hypothesis["weekday_mean_minutes"])
col2.metric("Avg Weekend (mins)", hypothesis["weekend_mean_minutes"])
col3.metric("p-value", hypothesis["p_value"])

st.markdown(f"**Finding:** {hypothesis['interpretation']}")
st.caption("Two-sample independent t-test, α = 0.05")

# ── Section 8: Listening Personality ─────────────────────────────────────────

st.markdown('<p class="section-header">🎭 Your Listening Personality</p>', unsafe_allow_html=True)

personality = get_listening_personality(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric("Most Loyal Artist", personality["most_loyal_artist"])
col2.metric("Listening Style", personality["listening_style"])
col3.metric("Peak Hour", personality["peak_listening_hour"])

col4, col5 = st.columns(2)
col4.metric("Most Active Month", personality["most_active_month"])
col5.metric("Overall Skip Rate", f"{personality['overall_skip_rate']}%")