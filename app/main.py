import json
import sys
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path
from collections import Counter

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
    analyze_time_of_day_listening,
    get_listening_personality,
    get_listening_streaks,
    get_monthly_breakdown,
    get_artist_loyalty_timeline,
    get_biggest_listening_day,
)
from src.spotify_api import fetch_all_metadata, compute_listening_age
from src.genre_map import enrich_genres_with_map, get_genre_listening_time

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Spotify Listening Tracker",
    page_icon="🎵",
    layout="wide",
)

st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Poppins', sans-serif; }
        .section-header {
            color: #1DB954;
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 2rem;
            font-family: 'Poppins', sans-serif;
        }
        .listening-age-card {
            background: linear-gradient(135deg, #1DB954 0%, #191414 100%);
            border-radius: 16px;
            padding: 2rem;
            text-align: center;
            margin: 1rem 0;
        }
        .listening-age-card h2 {
            color: white;
            font-size: 1.1rem;
            font-weight: 400;
            margin-bottom: 0.2rem;
            opacity: 0.85;
        }
        .listening-age-card h1 {
            color: white;
            font-size: 3rem;
            font-weight: 700;
            margin: 0.2rem 0;
        }
        .listening-age-card p {
            color: white;
            opacity: 0.75;
            font-size: 0.9rem;
            margin-top: 0.5rem;
        }
    </style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────

GREEN = "#1DB954"
RED = "#e74c3c"
CHART_THEME = dict(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="white")
MIN_YEAR_STREAMS = 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def hours_label(minutes: float) -> str:
    h = round(minutes / 60)
    return f"{h} hour" if h == 1 else f"{h} hours"


def horizontal_bar(data: pd.DataFrame, x_col: str, y_col: str, y_label: str, color: str = GREEN) -> go.Figure:
    data = data.copy()
    data["hours"] = data[x_col].apply(lambda m: round(m / 60))
    data["label"] = data[x_col].apply(hours_label)
    data = data.sort_values("hours")
    fig = px.bar(
        data,
        x="hours",
        y=y_col,
        text="label",
        orientation="h",
        labels={"hours": "Hours Played", y_col: y_label},
        color_discrete_sequence=[color],
    )
    fig.update_traces(textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    return fig


# ── File Upload ───────────────────────────────────────────────────────────────

@st.cache_data
def process_uploaded_files(files) -> pd.DataFrame:
    dataframes = []
    for file in files:
        data = json.load(file)
        dataframes.append(pd.DataFrame(data))
    combined = pd.concat(dataframes, ignore_index=True)
    return clean_streaming_data(combined)


if "df" not in st.session_state:
    st.title("🎵 Spotify Listening Tracker")
    st.markdown("Upload your Spotify extended streaming history to explore your listening habits.")
    st.markdown("### Upload Your Spotify Data")
    st.markdown(
        "To get your data: **Spotify → Account → Privacy Settings → Download your data** "
        "and request **Extended Streaming History**. Upload all `Streaming_History_Audio_*.json` files below."
    )
    uploaded_files = st.file_uploader(
        "Upload your Streaming_History_Audio JSON files",
        type="json",
        accept_multiple_files=True,
        help="Your data stays in your browser session and is never stored.",
    )
    if uploaded_files:
        with st.spinner("Loading and cleaning your listening history..."):
            st.session_state.df = process_uploaded_files(uploaded_files)
        st.rerun()
    else:
        st.info("⬆️ Upload your files above to get started.")
    st.stop()

df = st.session_state.df

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🎵 Spotify Listening Tracker")

# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("🎛️ Filters")
available_years = sorted(df["year"].unique().tolist())
selected_year = st.sidebar.selectbox(
    "Filter by Year",
    options=["All Time"] + available_years,
    help="Filter all charts and stats to a specific year, or view your entire history.",
)

if st.sidebar.button("🔄 Upload New Data"):
    del st.session_state.df
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 🌐 Spotify API")
st.sidebar.caption("Fetches release dates and genres. Only calls the API once — results are cached locally.")

fetch_api_data = st.sidebar.button(
    "Fetch Track & Genre Data",
    help="Enriches your data with song release dates and artist genres from Spotify.",
)

if fetch_api_data:
    with st.spinner("Fetching data from Spotify API... this may take a few minutes on first run."):
        try:
            release_years, genres = fetch_all_metadata(df)
            enriched_genres = enrich_genres_with_map(genres)
            st.session_state.release_years = release_years
            st.session_state.genres = enriched_genres
            st.sidebar.success("Done! Scroll down to see enriched data.")
        except Exception as e:
            st.sidebar.error(f"API fetch failed: {e}")

df_filtered = df if selected_year == "All Time" else df[df["year"] == selected_year]
year_param = None if selected_year == "All Time" else int(selected_year)

# ── Section 1: Overview ───────────────────────────────────────────────────────

st.markdown('<p class="section-header">📊 Overview</p>', unsafe_allow_html=True)

stats = get_overview_stats(df_filtered)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Hours Listened", f"{stats['total_hours_listened']:,}", help="Total hours of music played.")
col2.metric("Total Streams", f"{stats['total_streams']:,}", help="Number of times a track was played.")
col3.metric("Unique Artists", f"{stats['unique_artists']:,}", help="Number of distinct artists you listened to.")
col4.metric("Unique Tracks", f"{stats['unique_tracks']:,}", help="Number of distinct tracks you listened to.")
col5.metric("Unique Albums", f"{stats['unique_albums']:,}", help="Number of distinct albums you listened to.")

st.caption(f"Data from {stats['date_range_start']} to {stats['date_range_end']}")

# ── Section 2: Streaks ────────────────────────────────────────────────────────

st.markdown('<p class="section-header">🔥 Listening Streaks</p>', unsafe_allow_html=True)

streaks = get_listening_streaks(df_filtered)

col1, col2 = st.columns(2)
col1.metric(
    "Longest Streak",
    f"{streaks['longest_streak']} days",
    help="The longest number of consecutive days you listened to music.",
)
col2.metric(
    "Best Streak Period",
    f"{streaks['best_streak_start']} → {streaks['best_streak_end']}",
    help="The date range of your longest ever listening streak.",
)

# ── Section 3: Listening Age (API) ───────────────────────────────────────────

if "release_years" in st.session_state:
    st.markdown('<p class="section-header">🎂 Your Listening Age</p>', unsafe_allow_html=True)

    age_data = compute_listening_age(df_filtered, st.session_state.release_years)
    avg_age = age_data["avg_song_age_years"]
    avg_year = round(age_data["avg_release_year"])

    st.markdown(f"""
        <div class="listening-age-card">
            <h2>Your listening age is</h2>
            <h1>{avg_age} years</h1>
            <p>On average, the music you listen to was released around <strong>{avg_year}</strong></p>
        </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    col1.metric(
        "Oldest Track Found",
        f"{age_data['oldest_track']} ({age_data['oldest_year']})",
        help=f"By {age_data['oldest_artist']} — oldest among tracks Spotify returned release data for.",
    )
    col2.metric(
        "Newest Track Found",
        f"{age_data['newest_track']} ({age_data['newest_year']})",
        help=f"By {age_data['newest_artist']} — newest among tracks Spotify returned release data for.",
    )
    st.caption("Note: oldest and newest reflect only tracks with available Spotify release data, not your full history.")

# ── Section 4: Biggest Day ────────────────────────────────────────────────────

st.markdown('<p class="section-header">📅 Your Biggest Listening Day Ever</p>', unsafe_allow_html=True)

best_day = get_biggest_listening_day(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric("Date", best_day["date"], help="The day you listened to the most music.")
col2.metric("Hours Listened", hours_label(best_day["total_minutes"]), help="Total hours on your biggest day.")
col3.metric("Total Streams", f"{best_day['total_streams']:,}", help="Number of tracks played that day.")

if best_day["top_tracks"]:
    st.markdown(f"**What you were playing:** {' · '.join(best_day['top_tracks'])}")

# ── Section 5: Yearly Trend ───────────────────────────────────────────────────

if selected_year == "All Time":
    st.markdown('<p class="section-header">📈 Listening Over the Years</p>', unsafe_allow_html=True)

    yearly = get_yearly_trend(df_filtered)
    year_stream_counts = df_filtered.groupby("year")["track"].count()
    valid_years = year_stream_counts[year_stream_counts >= MIN_YEAR_STREAMS].index
    yearly = yearly[yearly["year"].isin(valid_years)]
    yearly["year"] = yearly["year"].astype(str)
    yearly["hours"] = yearly["total_minutes"].apply(lambda m: round(m / 60))
    yearly["label"] = yearly["total_minutes"].apply(hours_label)

    fig = px.bar(
        yearly,
        x="year",
        y="hours",
        text="label",
        labels={"hours": "Hours Listened", "year": "Year"},
        color_discrete_sequence=[GREEN],
    )
    fig.update_traces(textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME, xaxis_type="category")
    st.plotly_chart(fig, use_container_width=True)

# ── Section 6: Artist Loyalty Timeline ───────────────────────────────────────

if selected_year == "All Time":
    st.markdown('<p class="section-header">🎤 How Your Taste Evolved</p>', unsafe_allow_html=True)
    st.caption("Your #1 most played artist each year — see how your loyalties shifted over time.")

    timeline = get_artist_loyalty_timeline(df_filtered)
    timeline = timeline[timeline["year"].isin(valid_years)]
    timeline["year"] = timeline["year"].astype(str)
    timeline["hours"] = timeline["total_minutes"].apply(lambda m: round(m / 60))

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

# ── Section 7: Monthly Breakdown ─────────────────────────────────────────────

if selected_year != "All Time":
    st.markdown('<p class="section-header">📆 Month by Month</p>', unsafe_allow_html=True)

    monthly = get_monthly_breakdown(df_filtered, year_param)
    monthly["hours"] = monthly["total_minutes"].apply(lambda m: round(m / 60))
    monthly["label"] = monthly["total_minutes"].apply(hours_label)

    fig = px.bar(
        monthly,
        x="month_name",
        y="hours",
        text="label",
        labels={"hours": "Hours Listened", "month_name": "Month"},
        color_discrete_sequence=[GREEN],
    )
    fig.update_traces(textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    st.plotly_chart(fig, use_container_width=True)

# ── Section 8: Top Artists / Tracks / Albums / Genres / Nigerian ─────────────

st.markdown('<p class="section-header">🏆 Your Top Artists, Tracks & Albums</p>', unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs([
    "🎤 Artists", "🎵 Tracks", "💿 Albums", "🎸 Genres"
])

with tab1:
    top_artists = get_top_items(df_filtered, "artist", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_artists, "total_minutes", "artist", "Artist"), use_container_width=True)

with tab2:
    top_tracks = get_top_items(df_filtered, "track", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_tracks, "total_minutes", "track", "Track"), use_container_width=True)

with tab3:
    top_albums = get_top_items(df_filtered, "album", n=10, year=year_param)
    st.plotly_chart(horizontal_bar(top_albums, "total_minutes", "album", "Album"), use_container_width=True)

with tab4:
    if "genres" not in st.session_state:
        st.info("Click **Fetch Track & Genre Data** in the sidebar to load genre information.")
    else:
        genre_time_df = get_genre_listening_time(df_filtered, st.session_state.genres)

        if genre_time_df.empty:
            st.info("No genre data available for your listening history.")
        else:
            genre_time_df["label"] = genre_time_df["total_minutes"].apply(hours_label)
            fig = px.bar(
                genre_time_df.sort_values("total_minutes"),
                x="total_minutes",
                y="genre",
                text="label",
                orientation="h",
                labels={"total_minutes": "Minutes Listened", "genre": "Genre"},
                color_discrete_sequence=[GREEN],
            )
            fig.update_traces(textposition="inside", textfont_color="white")
            fig.update_layout(**CHART_THEME)
            st.plotly_chart(fig, use_container_width=True)

# ── Section 9: Listening Heatmap ─────────────────────────────────────────────

st.markdown('<p class="section-header">🗓️ When Do You Listen?</p>', unsafe_allow_html=True)
st.caption("Darker green = more listening time. Hover over any cell to see exact minutes.")

heatmap_data = get_hourly_heatmap_data(df_filtered)

fig = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=[f"{h}:00" for h in heatmap_data.columns],
    y=heatmap_data.index.tolist(),
    colorscale="Greens",
    hovertemplate="<b>%{y}</b> at <b>%{x}</b><br>%{z:.0f} minutes<extra></extra>",
))
fig.update_layout(xaxis_title="Hour of Day", yaxis_title="Day of Week", **CHART_THEME)
st.plotly_chart(fig, use_container_width=True)

# ── Section 10: Skip Analysis ─────────────────────────────────────────────────

st.markdown('<p class="section-header">⏭️ What Do You Skip?</p>', unsafe_allow_html=True)
st.caption(
    "Skip rate = percentage of streams where you hit next before the track finished. "
    "Only artists with 20+ streams are shown to keep this meaningful.",
)

skip_data = get_skip_analysis(df_filtered)

if skip_data.empty:
    st.info("Not enough streams per artist in the selected period to calculate meaningful skip rates.")
else:
    fig = px.bar(
        skip_data.sort_values("skip_rate"),
        x="skip_rate",
        y="artist",
        text="skip_rate",
        orientation="h",
        labels={"skip_rate": "Skip Rate (%)", "artist": "Artist"},
        color_discrete_sequence=[RED],
        hover_data={"total_streams": True, "total_skips": True},
    )
    fig.update_traces(texttemplate="%{text}%", textposition="inside", textfont_color="white")
    fig.update_layout(**CHART_THEME)
    st.plotly_chart(fig, use_container_width=True)

# ── Section 11: Platform Breakdown ───────────────────────────────────────────

st.markdown('<p class="section-header">📱 Where Do You Listen?</p>', unsafe_allow_html=True)
st.caption("Breakdown of your listening time by device type.")

platform_data = get_platform_breakdown(df_filtered)
platform_data["hours"] = platform_data["total_minutes"].apply(lambda m: round(m / 60))

fig = px.pie(
    platform_data,
    names="platform_category",
    values="hours",
    hole=0.4,
    color_discrete_sequence=px.colors.sequential.Greens_r,
)
fig.update_traces(
    texttemplate="%{label}<br>%{value} hours",
    hovertemplate="<b>%{label}</b><br>%{value} hours<br>%{percent}<extra></extra>",
)
fig.update_layout(paper_bgcolor="#0e1117", font_color="white")
st.plotly_chart(fig, use_container_width=True)

# ── Section 12: Weekend vs Weekday ────────────────────────────────────────────

st.markdown('<p class="section-header">📊 Weekend vs Weekday Listening</p>', unsafe_allow_html=True)

hypothesis = analyze_weekend_vs_weekday_listening(df_filtered)

weekday_hrs = round(hypothesis["weekday_mean_minutes"] / 60, 1)
weekend_hrs = round(hypothesis["weekend_mean_minutes"] / 60, 1)
diff_pct = round(abs(weekend_hrs - weekday_hrs) / max(weekday_hrs, 0.01) * 100)
more_on = "weekends" if weekend_hrs > weekday_hrs else "weekdays"

if hypothesis["significant"]:
    st.markdown(f"### You listen **{diff_pct}% more** on {more_on} on average.")
else:
    st.markdown("### Your listening is pretty consistent across weekdays and weekends.")

col1, col2 = st.columns(2)
col1.metric("Avg Weekday", f"{weekday_hrs} hours", help="Average hours listened per weekday.")
col2.metric("Avg Weekend Day", f"{weekend_hrs} hours", help="Average hours listened per weekend day.")

with st.expander("🔬 Show statistical details"):
    st.markdown(f"""
    This uses a **two-sample t-test** — a method that checks whether the gap between
    two averages is real or just random noise in your data.

    - **Weekday average:** {hypothesis['weekday_mean_minutes']} mins/day
    - **Weekend average:** {hypothesis['weekend_mean_minutes']} mins/day
    - **p-value:** {hypothesis['p_value']} — {'less than 0.05, so this difference is real and not due to chance.' if hypothesis['significant'] else 'greater than 0.05, so this difference could just be random variation.'}
    """)

# ── Section 13: Time of Day ───────────────────────────────────────────────────

st.markdown('<p class="section-header">🕐 Morning, Afternoon, Evening or Night?</p>', unsafe_allow_html=True)

tod = analyze_time_of_day_listening(df_filtered)

st.markdown(f"### You're a **{tod['dominant_period']}** listener {tod['dominant_emoji']}")
st.caption(tod["interpretation"])

period_df = pd.DataFrame([
    {"Period": p, "avg_minutes": tod["period_avgs"].get(p, 0)}
    for p in tod["period_order"]
])
period_df["hours"] = period_df["avg_minutes"].apply(lambda m: round(m / 60, 1))
period_df["label"] = period_df["avg_minutes"].apply(hours_label)

fig = px.bar(
    period_df,
    x="Period",
    y="hours",
    text="label",
    color="Period",
    color_discrete_map={
        "Morning": "#f39c12",
        "Afternoon": "#f1c40f",
        "Evening": "#e67e22",
        "Night": "#8e44ad",
    },
    labels={"hours": "Avg Hours per Day"},
)
fig.update_traces(textposition="inside", textfont_color="white", showlegend=False)
fig.update_layout(**CHART_THEME)
st.plotly_chart(fig, use_container_width=True)

with st.expander("🔬 Show statistical details"):
    st.markdown(f"""
    This uses a **one-way ANOVA test** — it checks whether the differences in listening
    across the four time periods are statistically meaningful or just random.

    - **Morning** (6am–12pm): {tod['period_avgs'].get('Morning', 0)} mins/day avg
    - **Afternoon** (12pm–6pm): {tod['period_avgs'].get('Afternoon', 0)} mins/day avg
    - **Evening** (6pm–10pm): {tod['period_avgs'].get('Evening', 0)} mins/day avg
    - **Night** (10pm–6am): {tod['period_avgs'].get('Night', 0)} mins/day avg
    - **p-value:** {tod['p_value']} — {'the differences are statistically significant.' if tod['significant'] else 'the differences are not statistically significant.'}
    """)

# ── Section 14: Listening Personality ────────────────────────────────────────

st.markdown('<p class="section-header">🎭 Your Listening Personality</p>', unsafe_allow_html=True)

personality = get_listening_personality(df_filtered)

col1, col2, col3 = st.columns(3)
col1.metric("Most Loyal Artist", personality["most_loyal_artist"], help="The artist you've streamed more than any other.")
col2.metric("Listening Style", personality["listening_style"], help="Based on what time of day you listen most.")
col3.metric("Peak Hour", personality["peak_listening_hour"], help="The hour you're most likely to be listening.")

col4, col5 = st.columns(2)
col4.metric("Most Active Month", personality["most_active_month"], help="The month you historically listen the most.")
col5.metric("Overall Skip Rate", f"{personality['overall_skip_rate']}%", help="How often you skip tracks across your entire history.")