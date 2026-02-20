import pytest
import pandas as pd
from src.analysis import (
    get_overview_stats,
    get_top_items,
    get_skip_analysis,
    analyze_weekend_vs_weekday_listening,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2024-01-13T14:00:00Z",  # Saturday
            "2024-01-13T15:00:00Z",  # Saturday
            "2024-01-15T09:00:00Z",  # Monday
            "2024-01-15T10:00:00Z",  # Monday
            "2024-01-16T11:00:00Z",  # Tuesday
        ], utc=True),
        "track": ["Track A", "Track B", "Track A", "Track C", "Track B"],
        "artist": ["Artist X", "Artist X", "Artist Y", "Artist Y", "Artist X"],
        "album": ["Album 1", "Album 1", "Album 2", "Album 2", "Album 1"],
        "minutes_played": [3.5, 4.0, 2.0, 3.0, 4.5],
        "skipped": [False, True, False, False, True],
        "year": [2024, 2024, 2024, 2024, 2024],
        "month_name": ["January"] * 5,
        "day_of_week": ["Saturday", "Saturday", "Monday", "Monday", "Tuesday"],
        "hour": [14, 15, 9, 10, 11],
        "date": pd.to_datetime(["2024-01-13", "2024-01-13", "2024-01-15", "2024-01-15", "2024-01-16"]).date,
        "platform_category": ["mobile", "mobile", "desktop", "desktop", "mobile"],
    })


def test_overview_stats_keys(sample_df):
    stats = get_overview_stats(sample_df)
    for key in ["total_hours_listened", "unique_tracks", "unique_artists", "total_streams"]:
        assert key in stats


def test_overview_total_streams(sample_df):
    stats = get_overview_stats(sample_df)
    assert stats["total_streams"] == 5


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