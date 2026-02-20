import json
import pandas as pd
from pathlib import Path


def load_streaming_files(raw_data_path: str | Path) -> pd.DataFrame:
    """
    Loads and combines all Spotify extended streaming history JSON files
    from the specified directory into a single DataFrame.

    Args:
        raw_data_path: Path to the folder containing the JSON files.

    Returns:
        A combined, minimally cleaned DataFrame of all streaming records.
    
    Raises:
        FileNotFoundError: If the directory doesn't exist.
        ValueError: If no valid streaming history files are found.
    """
    raw_data_path = Path(raw_data_path)

    if not raw_data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {raw_data_path}")

    streaming_files = list(raw_data_path.glob("Streaming_History_Audio*.json"))

    if not streaming_files:
        raise ValueError(f"No streaming history files found in {raw_data_path}")

    dataframes = []

    for file in streaming_files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            dataframes.append(pd.DataFrame(data))

    combined = pd.concat(dataframes, ignore_index=True)
    return combined


def clean_streaming_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the raw streaming DataFrame into an
    analysis-ready format.

    Args:
        df: Raw combined streaming DataFrame from load_streaming_files.

    Returns:
        Cleaned DataFrame with proper types and derived columns.
    """
    # Rename columns to friendlier names
    df = df.rename(columns={
        "ts": "timestamp",
        "ms_played": "ms_played",
        "master_metadata_track_name": "track",
        "master_metadata_album_artist_name": "artist",
        "master_metadata_album_album_name": "album",
        "spotify_track_uri": "track_uri",
        "reason_start": "reason_start",
        "reason_end": "reason_end",
        "shuffle": "shuffle",
        "skipped": "skipped",
        "platform": "platform",
    })

    # Parse timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Derive time-based columns
    df["date"] = df["timestamp"].dt.date
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["month_name"] = df["timestamp"].dt.strftime("%B")
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["hour"] = df["timestamp"].dt.hour

    # Convert ms to minutes
    df["minutes_played"] = (df["ms_played"] / 60000).round(2)

    # Drop rows where track is null (podcast remnants or local files)
    df = df.dropna(subset=["track", "artist"])

    # Clean platform column using str normalization
    df["platform"] = df["platform"].str.strip().str.lower()

    # Normalize platform names into cleaner categories
    df["platform_category"] = df["platform"].apply(_categorize_platform)

    # Sort by timestamp
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def _categorize_platform(platform: str) -> str:
    """
    Maps raw platform strings into clean categories.
    Uses string matching to handle Spotify's inconsistent platform naming.

    Args:
        platform: Raw platform string from Spotify data.

    Returns:
        Cleaned platform category string.
    """
    if not isinstance(platform, str):
        return "unknown"

    platform = platform.lower()

    if any(x in platform for x in ["android", "ios", "iphone", "mobile"]):
        return "mobile"
    elif any(x in platform for x in ["windows", "mac", "linux", "desktop"]):
        return "desktop"
    elif "web" in platform:
        return "web"
    elif "cast" in platform or "tv" in platform or "speaker" in platform:
        return "smart device"
    else:
        return "other"