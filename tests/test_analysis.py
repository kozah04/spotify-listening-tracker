import pytest
import pandas as pd
from src.analysis import (
    get_overview_stats,
    get_top_items,
    get_skip_analysis,
    analyze_weekend_vs_weekday_listening,
    get_listening_streaks,
    get_monthly_breakdown,
    get_artist_loyalty_timeline,
    get_biggest_listening_day,
    analyze_time_of_day_listening,
)

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2024-01-13T08:00:00Z",  # Saturday - Morning
            "2024-01-13T14:00:00Z",  # Saturday - Afternoon
            "2024-01-13T19:00:00Z",  # Saturday - Evening
            "2024-01-13T23:00:00Z",  # Saturday - Night
            "2024-01-15T09:00:00Z",  # Monday - Morning
            "2024-01-15T13:00:00Z",  # Monday - Afternoon
            "2024-01-15T20:00:00Z",  # Monday - Evening
            "2024-01-16T01:00:00Z",  # Tuesday - Night
        ], utc=True),
        "track": ["Track A", "Track B", "Track A", "Track C", "Track B", "Track A", "Track C", "Track B"],
        "artist": ["Artist X", "Artist X", "Artist Y", "Artist Y", "Artist X", "Artist Y", "Artist X", "Artist Y"],
        "album": ["Album 1", "Album 1", "Album 2", "Album 2", "Album 1", "Album 2", "Album 1", "Album 2"],
        "minutes_played": [3.5, 4.0, 2.0, 3.0, 4.5, 2.5, 3.5, 4.0],
        "skipped": [False, True, False, False, True, False, False, True],
        "year": [2024] * 8,
        "month_name": ["January"] * 8,
        "day_of_week": ["Saturday", "Saturday", "Saturday", "Saturday", "Monday", "Monday", "Monday", "Tuesday"],
        "hour": [8, 14, 19, 23, 9, 13, 20, 1],
        "date": pd.to_datetime([
            "2024-01-13", "2024-01-13", "2024-01-13", "2024-01-13",
            "2024-01-15", "2024-01-15", "2024-01-15", "2024-01-16"
        ]).date,
        "platform_category": ["mobile", "mobile", "desktop", "desktop", "mobile", "desktop", "mobile", "desktop"],
    })


def test_overview_stats_keys(sample_df):
    stats = get_overview_stats(sample_df)
    for key in ["total_hours_listened", "unique_tracks", "unique_artists", "total_streams"]:
        assert key in stats


def test_overview_total_streams(sample_df):
    stats = get_overview_stats(sample_df)
    assert stats["total_streams"] == 8

def test_get_top_items_returns_correct_n(sample_df):
    result = get_top_items(sample_df, "artist", n=2)
    assert len(result) <= 2


def test_get_top_items_sorted_descending(sample_df):
    result = get_top_items(sample_df, "artist")
    assert result["total_minutes"].is_monotonic_decreasing


def test_skip_analysis_filters_low_streams(sample_df):
    # All artists in sample have fewer than 20 streams so result should be empty
    result = get_skip_analysis(sample_df)
    assert len(result) == 0


def test_weekend_vs_weekday_returns_required_keys(sample_df):
    result = analyze_weekend_vs_weekday_listening(sample_df)
    for key in ["weekday_mean_minutes", "weekend_mean_minutes", "p_value", "significant", "interpretation"]:
        assert key in result


def test_weekend_vs_weekday_p_value_range(sample_df):
    result = analyze_weekend_vs_weekday_listening(sample_df)
    assert 0.0 <= result["p_value"] <= 1.0

def test_get_monthly_breakdown_ordered(sample_df):
    # Add a second month to sample
    extra = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-03-01T10:00:00Z"], utc=True),
        "track": ["Track D"],
        "artist": ["Artist Z"],
        "album": ["Album 3"],
        "minutes_played": [5.0],
        "skipped": [False],
        "year": [2024],
        "month_name": ["March"],
        "day_of_week": ["Friday"],
        "hour": [10],
        "date": pd.to_datetime(["2024-03-01"]).date,
        "platform_category": ["mobile"],
    })
    combined = pd.concat([sample_df, extra], ignore_index=True)
    result = get_monthly_breakdown(combined, 2024)
    months = result["month_name"].tolist()
    assert months.index("January") < months.index("March")


def test_get_artist_loyalty_timeline_one_per_year(sample_df):
    result = get_artist_loyalty_timeline(sample_df)
    assert len(result) == result["year"].nunique()


def test_get_biggest_listening_day_keys(sample_df):
    result = get_biggest_listening_day(sample_df)
    for key in ["date", "total_minutes", "total_hours", "top_tracks"]:
        assert key in result


def test_get_listening_streaks_keys(sample_df):
    result = get_listening_streaks(sample_df)
    for key in ["longest_streak", "current_streak", "best_streak_start", "best_streak_end"]:
        assert key in result


def test_get_listening_streaks_longest_positive(sample_df):
    result = get_listening_streaks(sample_df)
    assert result["longest_streak"] >= 1

def test_time_of_day_keys(sample_df):
    result = analyze_time_of_day_listening(sample_df)
    for key in ["period_avgs", "dominant_period", "p_value", "significant", "interpretation"]:
        assert key in result


def test_time_of_day_dominant_is_valid_period(sample_df):
    result = analyze_time_of_day_listening(sample_df)
    assert result["dominant_period"] in ["Morning", "Afternoon", "Evening", "Night"]


def test_time_of_day_p_value_range(sample_df):
    result = analyze_time_of_day_listening(sample_df)
    if result["p_value"] is not None:
        assert 0.0 <= result["p_value"] <= 1.0