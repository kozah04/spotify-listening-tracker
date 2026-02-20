import pytest
import pandas as pd
from pathlib import Path
from src.loader import clean_streaming_data, _categorize_platform


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_raw_df():
    """Minimal raw DataFrame mimicking Spotify's JSON structure."""
    return pd.DataFrame({
        "ts": ["2024-01-15T14:30:00Z", "2024-01-15T15:00:00Z", "2024-01-16T09:00:00Z"],
        "ms_played": [210000, 0, 185000],
        "master_metadata_track_name": ["Track A", "Track B", None],
        "master_metadata_album_artist_name": ["Artist X", "Artist Y", None],
        "master_metadata_album_album_name": ["Album 1", "Album 2", None],
        "spotify_track_uri": ["uri:a", "uri:b", "uri:c"],
        "reason_start": ["clickrow", "trackdone", "clickrow"],
        "reason_end": ["trackdone", "fwdbtn", "trackdone"],
        "shuffle": [True, False, True],
        "skipped": [False, True, False],
        "platform": ["Android", "Windows 10", None],
    })


# ── Tests ────────────────────────────────────────────────────────────────────

def test_clean_drops_null_tracks(sample_raw_df):
    cleaned = clean_streaming_data(sample_raw_df)
    assert cleaned["track"].isnull().sum() == 0


def test_clean_creates_time_columns(sample_raw_df):
    cleaned = clean_streaming_data(sample_raw_df)
    for col in ["year", "month", "hour", "day_of_week", "date"]:
        assert col in cleaned.columns


def test_clean_converts_ms_to_minutes(sample_raw_df):
    cleaned = clean_streaming_data(sample_raw_df)
    assert "minutes_played" in cleaned.columns
    assert cleaned["minutes_played"].iloc[0] == pytest.approx(3.5, rel=1e-2)


def test_clean_sorts_by_timestamp(sample_raw_df):
    cleaned = clean_streaming_data(sample_raw_df)
    assert cleaned["timestamp"].is_monotonic_increasing


def test_categorize_platform_mobile():
    assert _categorize_platform("Android") == "mobile"
    assert _categorize_platform("iOS") == "mobile"


def test_categorize_platform_desktop():
    assert _categorize_platform("Windows 10") == "desktop"
    assert _categorize_platform("macOS") == "desktop"


def test_categorize_platform_unknown():
    assert _categorize_platform(None) == "unknown"
    assert _categorize_platform("") == "other"


def test_load_raises_if_path_missing():
    from src.loader import load_streaming_files
    with pytest.raises(FileNotFoundError):
        load_streaming_files("nonexistent/path")