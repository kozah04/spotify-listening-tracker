import pytest
from unittest.mock import patch, MagicMock
from src.spotify_api import get_access_token, compute_listening_age
import pandas as pd


def test_get_access_token_raises_without_credentials():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="credentials not found"):
            get_access_token()


def test_compute_listening_age_basic():
    df = pd.DataFrame({
        "track": ["Song A", "Song B"],
        "artist": ["Artist X", "Artist Y"],
        "track_uri": ["spotify:track:aaa", "spotify:track:bbb"],
        "minutes_played": [10.0, 10.0],
    })
    release_years = {
        "spotify:track:aaa": 2000,
        "spotify:track:bbb": 2020,
    }
    result = compute_listening_age(df, release_years)
    assert "avg_song_age_years" in result
    assert "avg_release_year" in result
    assert result["oldest_year"] == 2000
    assert result["newest_year"] == 2020


def test_compute_listening_age_drops_missing_uris():
    df = pd.DataFrame({
        "track": ["Song A", "Song B"],
        "artist": ["Artist X", "Artist Y"],
        "track_uri": ["spotify:track:aaa", "spotify:track:zzz"],
        "minutes_played": [10.0, 10.0],
    })
    release_years = {"spotify:track:aaa": 2010}
    result = compute_listening_age(df, release_years)
    # Only one track matched so oldest and newest should both be 2010
    assert result["oldest_year"] == 2010
    assert result["newest_year"] == 2010