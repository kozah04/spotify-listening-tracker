import json
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.loader import clean_streaming_data
from src.analysis import (
    get_overview_stats,
    get_top_items,
    get_hourly_heatmap_data,
    get_skip_analysis,
    get_platform_breakdown,
    get_yearly_trend,
    analyze_weekend_vs_weekday_listening,
    get_listening_personality,
    get_listening_streaks,
    get_monthly_breakdown,
    get_artist_loyalty_timeline,
    get_biggest_listening_day,
)

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Spotify Listening Tracker",
    page_icon="🎵",
    layout="wide",
)

# ── Styles ────────────────────────────────────────────────────────────────────

st.markdown("""
    <style>
        .section-header {
            color: #1DB954;
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 2rem;
        }
        .personality-card {
            background: linear-gradient(135deg, #1DB954, #191414);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            margin-bottom: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🎵 Spotify Listening Tracker")
st.markdown("Upload your Spotify extended streaming history to explore your listening habits.")

# ── File Upload ───────────────────────────────────────────────────────────────

st.markdown("### Upload Your Spotify Data")
st.markdown(
    "To get your data: **Spotify → Account → Privacy Settings → Download your data** "
    "and request **Extended Streaming History**. Upload all `Streaming_History_Audio_*.json` files below."
)

uploaded_files = st.file_uploader(
    "Upload your Streaming_History_Audio JSON files",
    type="json",
    accept_multiple_files=True,
    help="You can upload multiple files at once. Your data stays in your browser session and is never stored.",
)

# ── Load and Process ──────────────────────────────────────────────────────────

@st.cache_data
def process_uploaded_files(files) -> pd.DataFrame:
    dataframes = []
    for file in files:
        data = json.load(file)
        dataframes.append(pd.DataFrame(data))
    combined = pd.concat(dataframes, ignore_index=True)
    return clean_streaming_data(combined)


if not uploaded_files:
    st.info("⬆️ Upload your files above to get started. Your data never leaves your browser session.")
    st.stop()

with st.spinner("Loading and cleaning your listening history..."):
    df = process_uploaded_files(uploaded_files)

# ── Sidebar Filters ───────────────────────────────────────────────────────────

st.sidebar.title("🎛️ Filters")
available_years = sorted(df["year"].unique().tolist())
selected_year = st.sidebar.selectbox(
    "Filter by Year",
    options=["All Time"] + available_years,
    help="Filter all charts and stats to a specific year, or view your entire history.",
)

df_filtered = df if selected_year == "All Time" else df[df["year"] == selected_year]
year_param = None if selected_year == "All Time" else int(selected_year)

CHART_THEME = dict(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
GREEN = "#1DB954"

# ── Section 1: Overview ───────────────────────────────────────────────────────

st.markdown('<p class="section-header">📊 Overview</p>', unsafe_allow_html=True)

stats = get_overview_stats(df_filtered)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Hours Listened", f"{stats['total_hours_listened']:,}", help="Total hours of music played in the selected period.")
col2.metric("Total Streams", f"{stats['total_streams']:,}", help="Number of times a track was played.")
col3.metric("Unique Artists", f"{stats['unique_artists']:,}", help="Number of distinct artists you listened to.")
col4.metric("Unique Tracks", f"{stats['unique_tracks']:,}", help="Number of distinct tracks you listened to.")
col5.metric("Unique Albums", f"{stats['unique_albums']:,}", help="Number of distinct albums you listened to.")

st.caption(f"Data from {stats['date_range_start']} to {stats['date_range_end']}")

# ── Section 2: Streaks ────────────────────────────────────────────────────────

st.markdown('<p class="section-header">🔥 Listening Streaks</p>', unsafe_allow_html=True)

streaks = get_listening_streaks(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric(
    "Longest Streak",
    f"{streaks['longest_streak']} days",
    help="The longest number of consecutive days you listened to music.",
)
col2.metric(
    "Current Streak",
    f"{streaks['current_streak']} days",
    help="How many consecutive days leading up to today you have listened.",
)
col3.metric(
    "Best Streak Period",
    f"{streaks['best_streak_start']} → {streaks['best_streak_end']}",
    help="The date range of your longest ever streak.",
)

# ── Section 3: Biggest Day ────────────────────────────────────────────────────

st.markdown('<p class="section-header">📅 Your Biggest Listening Day Ever</p>', unsafe_allow_html=True)

best_day = get_biggest_listening_day(df_filtered)

col1, col2 = st.columns(2)
col1.metric(
    "Date",
    best_day["date"],
    help="The single day in your history where you listened to the most music.",
)
col2.metric(
    "Hours Listened",
    f"{best_day['total_hours']} hrs",
    help="Total hours of music played on your biggest day.",
)

if best_day["top_tracks"]:
    st.markdown(f"**What you were playing:** {' · '.join(best_day['top_tracks'])}")

# ── Section 4: Yearly Trend ───────────────────────────────────────────────────

if selected_year == "All Time":
    st.markdown('<p class="section-header">📈 Listening Over the Years</p>', unsafe_allow_html=True)

    yearly = get_yearly_trend(df_filtered)
    yearly["year"] = yearly["year"].astype(str)  # prevents .5 year axis labels
    yearly["hours"] = (yearly["total_minutes"] / 60).round(1)

    fig = px.bar(
        yearly,
        x="year",
        y="hours",
        text="hours",
        labels={"hours": "Hours Listened", "year": "Year"},
        color_discrete_sequence=[GREEN],
    )
    fig.update_traces(texttemplate="%{text}h", textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME, xaxis_type="category")
    st.plotly_chart(fig, use_container_width=True)

# ── Section 5: Artist Loyalty Timeline ───────────────────────────────────────

if selected_year == "All Time":
    st.markdown('<p class="section-header">🎤 How Your Taste Evolved</p>', unsafe_allow_html=True)
    st.caption("Your #1 most played artist each year — see how your loyalties shifted over time.")

    timeline = get_artist_loyalty_timeline(df_filtered)
    timeline["year"] = timeline["year"].astype(str)
    timeline["hours"] = (timeline["total_minutes"] / 60).round(1)

    fig = px.bar(
        timeline,
        x="year",
        y="hours",
        text="artist",
        color="artist",
        labels={"hours": "Hours Listened", "year": "Year"},
    )
    fig.update_traces(textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME, xaxis_type="category", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

# ── Section 6: Monthly Breakdown ─────────────────────────────────────────────

if selected_year != "All Time":
    st.markdown('<p class="section-header">📆 Month by Month</p>', unsafe_allow_html=True)

    monthly = get_monthly_breakdown(df_filtered, year_param)
    monthly["hours"] = (monthly["total_minutes"] / 60).round(1)

    fig = px.bar(
        monthly,
        x="month_name",
        y="hours",
        text="hours",
        labels={"hours": "Hours Listened", "month_name": "Month"},
        color_discrete_sequence=[GREEN],
    )
    fig.update_traces(texttemplate="%{text}h", textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    st.plotly_chart(fig, use_container_width=True)

# ── Section 7: Top Artists / Tracks / Albums ──────────────────────────────────

st.markdown('<p class="section-header">🏆 Your Top Artists, Tracks & Albums</p>', unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🎤 Artists", "🎵 Tracks", "💿 Albums"])

def horizontal_bar(data: pd.DataFrame, x: str, y: str, label: str):
    data = data.copy()
    data["hours"] = (data[x] / 60).round(1)
    data = data.sort_values("hours")
    fig = px.bar(
        data,
        x="hours",
        y=y,
        text="hours",
        orientation="h",
        labels={"hours": "Hours Played", y: label},
        color_discrete_sequence=[GREEN],
    )
    fig.update_traces(texttemplate="%{text}h", textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    return fig

with tab1:
    top_artists = get_top_items(df_filtered, "artist", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_artists, "total_minutes", "artist", "Artist"), use_container_width=True)

with tab2:
    top_tracks = get_top_items(df_filtered, "track", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_tracks, "total_minutes", "track", "Track"), use_container_width=True)

with tab3:
    top_albums = get_top_items(df_filtered, "album", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_albums, "total_minutes", "album", "Album"), use_container_width=True)

# ── Section 8: Listening Heatmap ──────────────────────────────────────────────

st.markdown('<p class="section-header">🗓️ When Do You Listen?</p>', unsafe_allow_html=True)
st.caption("Darker green = more listening time. Hover over any cell to see the exact minutes.")

heatmap_data = get_hourly_heatmap_data(df_filtered)

fig = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=[f"{h}:00" for h in heatmap_data.columns],
    y=heatmap_data.index.tolist(),
    colorscale="Greens",
    hovertemplate="<b>%{y}</b> at <b>%{x}</b><br>%{z:.0f} minutes<extra></extra>",
))
fig.update_layout(
    xaxis_title="Hour of Day",
    yaxis_title="Day of Week",
    **CHART_THEME,
)
st.plotly_chart(fig, use_container_width=True)

# ── Section 9: Skip Analysis ──────────────────────────────────────────────────

st.markdown('<p class="section-header">⏭️ What Do You Skip?</p>', unsafe_allow_html=True)
st.caption(
    "Skip rate = percentage of streams where you skipped the track. "
    "Only artists with 20+ streams are included to keep this meaningful.",
)

skip_data = get_skip_analysis(df_filtered)

if skip_data.empty:
    st.info("Not enough streams per artist in the selected period to calculate meaningful skip rates.")
else:
    skip_data = skip_data.sort_values("skip_rate")
    fig = px.bar(
        skip_data,
        x="skip_rate",
        y="artist",
        text="skip_rate",
        orientation="h",
        labels={"skip_rate": "Skip Rate (%)", "artist": "Artist"},
        color_discrete_sequence=["#e74c3c"],
        hover_data={"total_streams": True, "total_skips": True},
    )
    fig.update_traces(texttemplate="%{text}%", textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    st.plotly_chart(fig, use_container_width=True)

# ── Section 10: Platform Breakdown ───────────────────────────────────────────

st.markdown('<p class="section-header">📱 Where Do You Listen?</p>', unsafe_allow_html=True)
st.caption("Breakdown of your listening time by device type.")

platform_data = get_platform_breakdown(df_filtered)
platform_data["hours"] = (platform_data["total_minutes"] / 60).round(1)

fig = px.pie(
    platform_data,
    names="platform_category",
    values="hours",
    hole=0.4,
    color_discrete_sequence=px.colors.sequential.Greens_r,
)
fig.update_traces(
    texttemplate="%{label}<br>%{value}h",
    hovertemplate="<b>%{label}</b><br>%{value} hours<br>%{percent}<extra></extra>",
)
fig.update_layout(paper_bgcolor="#0e1117", font_color="white")
st.plotly_chart(fig, use_container_width=True)

# ── Section 11: Weekend vs Weekday ────────────────────────────────────────────

st.markdown('<p class="section-header">📊 Weekend vs Weekday Listening</p>', unsafe_allow_html=True)

hypothesis = analyze_weekend_vs_weekday_listening(df_filtered)

weekday_hrs = round(hypothesis["weekday_mean_minutes"] / 60, 1)
weekend_hrs = round(hypothesis["weekend_mean_minutes"] / 60, 1)
diff_pct = round(abs(weekend_hrs - weekday_hrs) / max(weekday_hrs, 0.01) * 100, 1)
more_on = "weekends" if weekend_hrs > weekday_hrs else "weekdays"

# Plain English headline
if hypothesis["significant"]:
    st.markdown(f"### You listen **{diff_pct}% more** on {more_on} on average.")
else:
    st.markdown("### Your listening is pretty consistent across weekdays and weekends.")

col1, col2 = st.columns(2)
col1.metric("Avg Weekday", f"{weekday_hrs} hrs", help="Average hours listened per weekday.")
col2.metric("Avg Weekend Day", f"{weekend_hrs} hrs", help="Average hours listened per weekend day.")

with st.expander("🔬 Show statistical details"):
    st.markdown(f"""
    This finding is based on a **two-sample t-test**, a statistical method that compares 
    the average listening time on weekdays vs weekends across your entire history.
    
    - **Weekday average:** {hypothesis['weekday_mean_minutes']} mins/day  
    - **Weekend average:** {hypothesis['weekend_mean_minutes']} mins/day  
    - **p-value:** {hypothesis['p_value']} — {'this difference is statistically significant, meaning it is very unlikely to be due to random chance.' if hypothesis['significant'] else 'this difference is not statistically significant, meaning it could just be random variation.'}
    - **Significance level (α):** 0.05
    """)

# ── Section 12: Listening Personality ────────────────────────────────────────

st.markdown('<p class="section-header">🎭 Your Listening Personality</p>', unsafe_allow_html=True)

personality = get_listening_personality(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric(
    "Most Loyal Artist",
    personality["most_loyal_artist"],
    help="The artist you have streamed more than any other.",
)
col2.metric(
    "Listening Style",
    personality["listening_style"],
    help="Based on what time of day you do most of your listening.",
)
col3.metric(
    "Peak Hour",
    personality["peak_listening_hour"],
    help="The hour of the day you are most likely to be listening.",
)

col4, col5 = st.columns(2)
col4.metric(
    "Most Active Month",
    personality["most_active_month"],
    help="The calendar month where you historically listen the most.",
)
col5.metric(
    "Overall Skip Rate",
    f"{personality['overall_skip_rate']}%",
    help="Percentage of tracks you skipped before they finished across your entire history.",
)