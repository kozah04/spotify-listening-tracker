import pandas as pd
from collections import Counter

# Manual genre mapping for artists where Spotify's API returns empty genre data.
GENRE_MAP: dict[str, list[str]] = {
    # ── Your Top 50 Artists ───────────────────────────────────────────────────
    "NF": ["hip hop", "christian hip hop", "rap"],
    "Billie Eilish": ["pop", "alternative", "indie pop"],
    "Eminem": ["hip hop", "rap"],
    "Tom Odell": ["indie pop", "alternative", "singer-songwriter"],
    "Ed Sheeran": ["pop", "singer-songwriter"],
    "Kendrick Lamar": ["hip hop", "rap", "conscious hip hop"],
    "Anne-Marie": ["pop", "dance pop"],
    "Wizkid": ["afrobeats", "afropop"],
    "Lana Del Rey": ["indie pop", "dream pop", "alternative"],
    "Burna Boy": ["afrobeats", "afrofusion"],
    "Harry Styles": ["pop", "rock", "indie pop"],
    "SZA": ["r&b", "pop", "alternative r&b"],
    "Yasuharu Takanashi": ["anime soundtrack", "orchestral", "soundtrack"],
    "Michael Jackson": ["pop", "r&b", "soul", "funk"],
    "J. Cole": ["hip hop", "rap", "conscious hip hop"],
    "Cigarettes After Sex": ["dream pop", "ambient pop", "indie pop"],
    "Coldplay": ["pop", "alternative rock", "indie rock"],
    "Tyla": ["afrobeats", "afropop", "r&b"],
    "Lewis Capaldi": ["pop", "singer-songwriter", "soul"],
    "Niall Horan": ["pop", "singer-songwriter"],
    "One Direction": ["pop"],
    "Drake": ["hip hop", "rap", "r&b"],
    "Justin Bieber": ["pop", "r&b", "dance pop"],
    "P-Square": ["afrobeats", "afropop", "r&b"],
    "Hillsong Worship": ["christian", "worship", "contemporary christian"],
    "The Weeknd": ["r&b", "pop", "alternative r&b"],
    "Sia": ["pop", "dance pop", "electropop"],
    "Post Malone": ["pop", "hip hop", "trap"],
    "Bruno Mars": ["pop", "r&b", "funk", "soul"],
    "Rema": ["afrobeats", "afropop"],
    "Dave": ["hip hop", "rap", "uk rap"],
    "Asake": ["afrobeats", "street pop"],
    "Adele": ["pop", "soul", "singer-songwriter"],
    "Rihanna": ["r&b", "pop", "dance pop"],
    "d4vd": ["indie pop", "alternative r&b", "bedroom pop"],
    "Metro Boomin": ["hip hop", "trap", "rap"],
    "Omah Lay": ["afrobeats", "afropop"],
    "Labrinth": ["r&b", "pop", "alternative"],
    "Hans Zimmer": ["soundtrack", "orchestral", "classical"],
    "RAYE": ["pop", "r&b", "soul"],
    "Sabrina Carpenter": ["pop", "singer-songwriter"],
    "Lauren Spencer Smith": ["pop", "soul", "singer-songwriter"],
    "Childish Gambino": ["hip hop", "r&b", "indie", "funk"],
    "Kanye West": ["hip hop", "rap", "alternative hip hop"],
    "Tems": ["afrobeats", "afrosoul", "r&b"],
    "2Baba": ["afrobeats", "afropop"],
    "Chris Brown": ["r&b", "pop", "dance pop"],
    "Tame Impala": ["psychedelic rock", "indie rock", "alternative"],
    "Conan Gray": ["indie pop", "singer-songwriter", "alternative"],
    "Russ": ["hip hop", "r&b", "rap"],

    # ── Additional Nigerian / African Artists ─────────────────────────────────
    "Davido": ["afrobeats", "afropop"],
    "Fireboy DML": ["afrobeats", "afropop"],
    "Ayra Starr": ["afrobeats", "afropop"],
    "Kizz Daniel": ["afrobeats", "afropop"],
    "Flavour": ["afrobeats", "highlife"],
    "Tekno": ["afrobeats", "afropop"],
    "Yemi Alade": ["afrobeats", "afropop"],
    "Mr Eazi": ["afrobeats", "banku music"],
    "Joeboy": ["afrobeats", "afropop"],
    "Johnny Drille": ["afrobeats", "alternative"],
    "Adekunle Gold": ["afrobeats", "afropop", "alternative"],
    "Simi": ["afrobeats", "afropop", "r&b"],
    "Falz": ["afrobeats", "hip hop"],
    "Olamide": ["afrobeats", "street pop", "hip hop"],
    "Phyno": ["afrobeats", "hip hop"],
    "Zlatan": ["afrobeats", "street pop"],
    "Fela Kuti": ["afrobeats", "highlife", "afrojuju"],
    "Oxlade": ["afrobeats", "afrosoul", "r&b"],
    "Ladipoe": ["afrobeats", "hip hop"],
    "Blaqbonez": ["afrobeats", "hip hop"],
    "Shallipopi": ["afrobeats", "street pop"],
    "Seyi Vibez": ["afrobeats", "street pop"],
    "Ruger": ["afrobeats", "afropop"],
    "BNXN": ["afrobeats", "afropop", "r&b"],
    "Amaarae": ["afrobeats", "alternative", "r&b"],
    "Tiwa Savage": ["afrobeats", "afropop"],
    "Wande Coal": ["afrobeats", "afropop", "r&b"],
    "Patoranking": ["afrobeats", "reggae dancehall"],
    "Timaya": ["afrobeats", "afropop"],
    "D'banj": ["afrobeats", "afropop"],
    "Don Jazzy": ["afrobeats", "afropop"],
    "Victor AD": ["afrobeats", "afropop"],
    "Lojay": ["afrobeats", "afropop"],
    "Fave": ["afrobeats", "afropop"],
    "Naira Marley": ["afrobeats", "street pop"],
    "Portable": ["afrobeats", "street pop"],
    "Cruel Santino": ["afrobeats", "alternative", "r&b"],
    "King Sunny Ade": ["highlife", "juju"],
    "Chief Commander Ebenezer Obey": ["highlife", "juju"],
    "Lagbaja": ["afrobeats", "afrojuju"],
    "2Face Idibia": ["afrobeats", "afropop"],
    "Seun Kuti": ["afrobeats", "afrojuju"],
    "Femi Kuti": ["afrobeats", "afrojuju"],
    "Oliver De Coque": ["highlife"],

    # ── Ghana ─────────────────────────────────────────────────────────────────
    "Sarkodie": ["hiplife", "hip hop"],
    "Stonebwoy": ["afrobeats", "reggae dancehall"],
    "Shatta Wale": ["afrobeats", "reggae dancehall"],
    "Black Sherif": ["afrobeats", "hip hop"],
    "KiDi": ["afrobeats", "afropop"],
    "Kuami Eugene": ["afrobeats", "afropop"],

    # ── East Africa ───────────────────────────────────────────────────────────
    "Diamond Platnumz": ["bongo flava", "afrobeats"],
    "Sauti Sol": ["afropop", "r&b"],

    # ── South Africa ──────────────────────────────────────────────────────────
    "Nasty C": ["hip hop", "rap"],
    "AKA": ["hip hop", "rap"],
    "Cassper Nyovest": ["hip hop", "rap"],
    "Master KG": ["afrobeats", "amapiano"],
    "Kabza De Small": ["amapiano"],
    "DJ Maphorisa": ["amapiano"],
    "Focalistic": ["amapiano", "hip hop"],
    "Ami Faku": ["afropop", "r&b"],

    # ── Global ────────────────────────────────────────────────────────────────
    "Bad Bunny": ["latin", "reggaeton"],
    "21 Savage": ["hip hop", "rap", "trap"],
    "Future": ["hip hop", "trap"],
    "Lil Baby": ["hip hop", "trap"],
    "Gunna": ["hip hop", "trap"],
    "Doja Cat": ["pop", "r&b", "hip hop"],
    "Lizzo": ["pop", "r&b"],
    "Ariana Grande": ["pop", "r&b"],
    "Dua Lipa": ["pop", "dance"],
    "Taylor Swift": ["pop", "country"],
    "Beyoncé": ["r&b", "pop"],
    "Travis Scott": ["hip hop", "rap", "trap"],
}

# Comprehensive set of Nigerian artists for nationality filtering
NIGERIAN_ARTISTS: set[str] = {
    # Legends
    "Fela Kuti", "King Sunny Ade", "Chief Commander Ebenezer Obey",
    "Oliver De Coque", "Lagbaja", "Seun Kuti", "Femi Kuti",

    # Second generation
    "2Baba", "2Face Idibia", "P-Square", "D'banj", "Don Jazzy",
    "Davido", "Wizkid", "Burna Boy", "Olamide", "Phyno",
    "Flavour", "Tekno", "Yemi Alade", "Tiwa Savage", "Wande Coal",
    "Patoranking", "Timaya", "Harrysong", "Iyanya", "Kizz Daniel",

    # Current generation
    "Asake", "Rema", "Fireboy DML", "Omah Lay", "Tems",
    "Ckay", "Ayra Starr", "Joeboy", "Johnny Drille", "Adekunle Gold",
    "Simi", "Falz", "Ladipoe", "Blaqbonez", "Cruel Santino",
    "Victor AD", "Oxlade", "Lojay", "Ruger", "BNXN",
    "Shallipopi", "Seyi Vibez", "Zlatan", "Naira Marley", "Portable",
    "Fave", "Tyla", "Amaarae",
}


def enrich_genres_with_map(spotify_genres: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Merges Spotify API genre results with our manual genre map.
    Manual map takes precedence when Spotify returns empty lists.

    Args:
        spotify_genres: Dict of artist_name -> genre list from Spotify API.

    Returns:
        Enriched dict with manual genres filled in where Spotify had none.
    """
    enriched = {}

    for artist, genres in spotify_genres.items():
        if genres:
            enriched[artist] = genres
        elif artist in GENRE_MAP:
            enriched[artist] = GENRE_MAP[artist]
        else:
            enriched[artist] = []

    for artist, genres in GENRE_MAP.items():
        if artist not in enriched:
            enriched[artist] = genres

    return enriched


def get_top_nigerian_artists(df: pd.DataFrame, genre_map: dict, n: int = 20) -> pd.DataFrame:
    """
    Filters listening data to Nigerian artists only and returns
    top n by total minutes played.

    Args:
        df: Cleaned streaming DataFrame.
        genre_map: Enriched genre dictionary.
        n: Number of top artists to return.

    Returns:
        DataFrame with artist, total_minutes, and genres.
    """
    nigerian_df = df[df["artist"].isin(NIGERIAN_ARTISTS)]

    if nigerian_df.empty:
        return pd.DataFrame(columns=["artist", "total_minutes", "genres"])

    top = (
        nigerian_df.groupby("artist")["minutes_played"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )

    top["genres"] = top["artist"].map(
        lambda a: ", ".join(genre_map.get(a, [])) or "untagged"
    )

    return top

def get_genre_listening_time(df: pd.DataFrame, genre_map: dict[str, list[str]], n: int = 15) -> pd.DataFrame:
    """
    Computes total minutes listened per genre, weighted by actual streaming time.
    Each artist's minutes are distributed equally across their genres.

    Args:
        df: Cleaned streaming DataFrame.
        genre_map: Enriched genre dictionary mapping artist -> genre list.
        n: Number of top genres to return.

    Returns:
        DataFrame with genre and total_minutes columns.
    """
    rows = []

    for _, row in df.iterrows():
        artist = row["artist"]
        minutes = row["minutes_played"]
        genres = genre_map.get(artist, [])

        if not genres:
            continue

        # Distribute minutes equally across the artist's genres
        minutes_per_genre = minutes / len(genres)
        for genre in genres:
            rows.append({"genre": genre, "minutes_played": minutes_per_genre})

    if not rows:
        return pd.DataFrame(columns=["genre", "total_minutes"])

    genre_df = pd.DataFrame(rows)
    return (
        genre_df.groupby("genre")["minutes_played"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )