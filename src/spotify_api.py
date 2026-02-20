import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()


def get_access_token() -> str:
    """
    Fetches a Spotify API access token using the Client Credentials flow.
    This flow doesn't require user login — it's for accessing public data only.

    Returns:
        A valid access token string.

    Raises:
        ValueError: If credentials are missing from environment.
        RuntimeError: If the token request fails.
    """
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError(
            "Spotify credentials not found. Make sure SPOTIFY_CLIENT_ID and "
            "SPOTIFY_CLIENT_SECRET are set in your .env file."
        )

    response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
    )

    if response.status_code != 200:
        raise RuntimeError(f"Failed to get Spotify token: {response.status_code} {response.text}")

    return response.json()["access_token"]


def get_track_metadata(track_uris: list[str], token: str) -> dict:
    """
    Fetches track metadata (release date) for a list of Spotify track URIs.
    Processes in batches of 50 as per Spotify API limits.

    Args:
        track_uris: List of Spotify track URIs e.g. 'spotify:track:XXXX'
        token: Valid Spotify access token.

    Returns:
        Dictionary mapping track_uri -> release_year (int or None)
    """
    # Extract track IDs from URIs
    track_ids = [uri.split(":")[-1] for uri in track_uris if isinstance(uri, str)]
    uri_to_id = {uri: uri.split(":")[-1] for uri in track_uris if isinstance(uri, str)}
    id_to_uri = {v: k for k, v in uri_to_id.items()}

    results = {}
    batch_size = 50
    headers = {"Authorization": f"Bearer {token}"}

    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i: i + batch_size]
        response = requests.get(
            "https://api.spotify.com/v1/tracks",
            headers=headers,
            params={"ids": ",".join(batch)},
        )

        if response.status_code == 429:
            # Rate limited — wait and retry once
            retry_after = int(response.headers.get("Retry-After", 5))
            time.sleep(retry_after)
            response = requests.get(
                "https://api.spotify.com/v1/tracks",
                headers=headers,
                params={"ids": ",".join(batch)},
            )

        if response.status_code != 200:
            continue

        for track in response.json().get("tracks", []):
            if not track:
                continue
            track_id = track["id"]
            uri = id_to_uri.get(track_id)
            release_date = track.get("album", {}).get("release_date", "")

            # Release date can be YYYY, YYYY-MM, or YYYY-MM-DD
            try:
                year = int(release_date[:4])
            except (ValueError, TypeError):
                year = None

            if uri:
                results[uri] = year

        # Be polite to the API
        time.sleep(0.1)

    return results


def get_artist_genres(artist_names: list[str], token: str) -> dict:
    """
    Fetches genres for a list of artist names by searching the Spotify API.

    Args:
        artist_names: List of artist name strings.
        token: Valid Spotify access token.

    Returns:
        Dictionary mapping artist_name -> list of genre strings.
    """
    results = {}
    headers = {"Authorization": f"Bearer {token}"}

    for artist in artist_names:
        response = requests.get(
            "https://api.spotify.com/v1/search",
            headers=headers,
            params={"q": artist, "type": "artist", "limit": 1},
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            time.sleep(retry_after)
            response = requests.get(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params={"q": artist, "type": "artist", "limit": 1},
            )

        if response.status_code != 200:
            results[artist] = []
            continue

        items = response.json().get("artists", {}).get("items", [])
        if items:
            results[artist] = items[0].get("genres", [])
        else:
            results[artist] = []

        time.sleep(0.1)

    return results


def compute_listening_age(df, track_release_years: dict) -> dict:
    """
    Computes the average age of songs a user listens to,
    weighted by minutes played.

    Args:
        df: Cleaned streaming DataFrame.
        track_release_years: Dict mapping track_uri -> release_year.

    Returns:
        Dictionary with average song age, average release year, and oldest/newest tracks.
    """
    import pandas as pd
    from datetime import datetime

    current_year = datetime.now().year

    df = df.copy()
    df["release_year"] = df["track_uri"].map(track_release_years)
    df = df.dropna(subset=["release_year"])
    df["release_year"] = df["release_year"].astype(int)
    df["song_age"] = current_year - df["release_year"]

    # Weighted average by minutes played
    weighted_avg_age = (
        (df["song_age"] * df["minutes_played"]).sum() / df["minutes_played"].sum()
    )
    weighted_avg_year = current_year - weighted_avg_age

    oldest = df.loc[df["release_year"].idxmin(), ["track", "artist", "release_year"]]
    newest = df.loc[df["release_year"].idxmax(), ["track", "artist", "release_year"]]

    return {
        "avg_song_age_years": round(weighted_avg_age, 1),
        "avg_release_year": round(weighted_avg_year, 1),
        "oldest_track": oldest["track"],
        "oldest_artist": oldest["artist"],
        "oldest_year": int(oldest["release_year"]),
        "newest_track": newest["track"],
        "newest_artist": newest["artist"],
        "newest_year": int(newest["release_year"]),
    }