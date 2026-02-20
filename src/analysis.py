import pandas as pd
import numpy as np
from scipy import stats


def get_overview_stats(df: pd.DataFrame) -> dict:
    """
    Computes high-level listening statistics.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary of summary statistics.
    """
    total_minutes = df["minutes_played"].sum()
    total_hours = round(total_minutes / 60, 1)
    total_days = round(total_minutes / 1440, 1)

    return {
        "total_hours_listened": total_hours,
        "total_days_listened": total_days,
        "total_streams": len(df),
        "unique_tracks": df["track"].nunique(),
        "unique_artists": df["artist"].nunique(),
        "unique_albums": df["album"].nunique(),
        "date_range_start": str(df["timestamp"].min().date()),
        "date_range_end": str(df["timestamp"].max().date()),
        "most_active_year": int(df["year"].value_counts().idxmax()),
    }


def get_top_items(df: pd.DataFrame, column: str, n: int = 10, year: int = None) -> pd.DataFrame:
    """
    Returns the top n artists, tracks, or albums by total minutes played.

    Args:
        df: Cleaned streaming DataFrame.
        column: One of 'artist', 'track', or 'album'.
        n: Number of top items to return.
        year: Optional year filter.

    Returns:
        DataFrame with item name and total minutes played.
    """
    if year:
        df = df[df["year"] == year]

    return (
        df.groupby(column)["minutes_played"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )


def get_hourly_heatmap_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a pivot table of listening activity by day of week and hour.
    Suitable for rendering as a heatmap.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Pivot DataFrame with days as rows and hours as columns.
    """
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    heatmap = (
        df.groupby(["day_of_week", "hour"])["minutes_played"]
        .sum()
        .reset_index()
        .pivot(index="day_of_week", columns="hour", values="minutes_played")
        .fillna(0)
        .reindex(day_order)
    )

    return heatmap


def get_skip_analysis(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Identifies the most skipped artists by skip rate.
    Only considers artists with at least 20 streams to avoid noise.

    Args:
        df: Cleaned streaming DataFrame.
        n: Number of artists to return.

    Returns:
        DataFrame with artist, total streams, skips, and skip rate.
    """
    artist_stats = df.groupby("artist").agg(
        total_streams=("track", "count"),
        total_skips=("skipped", "sum")
    ).reset_index()

    # Filter out artists with too few streams to be meaningful
    artist_stats = artist_stats[artist_stats["total_streams"] >= 20]

    artist_stats["skip_rate"] = (
        artist_stats["total_skips"] / artist_stats["total_streams"] * 100
    ).round(1)

    return artist_stats.sort_values("skip_rate", ascending=False).head(n)


def get_platform_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarizes listening minutes and stream count by platform category.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        DataFrame with platform stats.
    """
    return (
        df.groupby("platform_category")
        .agg(
            total_minutes=("minutes_played", "sum"),
            total_streams=("track", "count")
        )
        .reset_index()
        .sort_values("total_minutes", ascending=False)
    )


def get_yearly_trend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns total minutes listened per year.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        DataFrame with year and total minutes.
    """
    return (
        df.groupby("year")["minutes_played"]
        .sum()
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )


def analyze_weekend_vs_weekday_listening(df: pd.DataFrame) -> dict:
    """
    Performs a two-sample t-test to determine whether daily listening time
    is significantly different on weekends vs weekdays.

    H0: Mean daily listening time is equal on weekdays and weekends.
    H1: Mean daily listening time differs between weekdays and weekends.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary with test results and plain English interpretation.
    """
    weekend_days = ["Saturday", "Sunday"]

    daily = df.groupby(["date", "day_of_week"])["minutes_played"].sum().reset_index()
    daily["is_weekend"] = daily["day_of_week"].isin(weekend_days)

    weekday_listening = daily[~daily["is_weekend"]]["minutes_played"]
    weekend_listening = daily[daily["is_weekend"]]["minutes_played"]

    t_stat, p_value = stats.ttest_ind(weekend_listening, weekday_listening)

    alpha = 0.05
    significant = p_value < alpha

    return {
        "weekday_mean_minutes": round(weekday_listening.mean(), 2),
        "weekend_mean_minutes": round(weekend_listening.mean(), 2),
        "t_statistic": round(t_stat, 4),
        "p_value": round(p_value, 4),
        "significant": significant,
        "interpretation": (
            f"You listen significantly more on {'weekends' if weekend_listening.mean() > weekday_listening.mean() else 'weekdays'} "
            f"(p={round(p_value, 4)}, α=0.05). This difference is unlikely to be due to chance."
            if significant else
            f"There is no statistically significant difference between your weekend and weekday listening "
            f"(p={round(p_value, 4)}, α=0.05)."
        )
    }


def get_listening_personality(df: pd.DataFrame) -> dict:
    """
    Generates a fun personality summary based on listening patterns.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary of personality insights.
    """
    # Most loyal artist — most streamed overall
    top_artist = df["artist"].value_counts().idxmax()

    # Night owl score — % of listening between 10pm and 4am
    night_hours = df[df["hour"].isin(range(22, 24)) | df["hour"].isin(range(0, 4))]
    night_owl_score = round(len(night_hours) / len(df) * 100, 1)

    # Peak hour
    peak_hour = int(df["hour"].value_counts().idxmax())
    peak_hour_label = f"{peak_hour}:00 - {peak_hour+1}:00"

    # Most active month across all years
    peak_month = df["month_name"].value_counts().idxmax()

    # Skip tendency
    overall_skip_rate = round(df["skipped"].sum() / len(df) * 100, 1)

    return {
        "most_loyal_artist": top_artist,
        "night_owl_score": night_owl_score,
        "peak_listening_hour": peak_hour_label,
        "most_active_month": peak_month,
        "overall_skip_rate": overall_skip_rate,
        "listening_style": (
            "Night Owl 🦉" if night_owl_score > 20
            else "Early Bird 🐦" if peak_hour < 10
            else "Daytime Listener ☀️"
        )
    }

def get_listening_streaks(df: pd.DataFrame) -> dict:
    """
    Calculates longest and current listening streaks in days.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary with longest streak, current streak, and best streak dates.
    """
    active_dates = sorted(df["date"].unique())
    active_dates = [pd.Timestamp(d) for d in active_dates]

    if not active_dates:
        return {"longest_streak": 0, "current_streak": 0, "best_streak_start": None, "best_streak_end": None}

    longest = 1
    current = 1
    best_start = active_dates[0]
    best_end = active_dates[0]
    streak_start = active_dates[0]

    for i in range(1, len(active_dates)):
        diff = (active_dates[i] - active_dates[i - 1]).days
        if diff == 1:
            current += 1
            if current > longest:
                longest = current
                best_start = streak_start
                best_end = active_dates[i]
        else:
            current = 1
            streak_start = active_dates[i]

    # Calculate current streak from today backwards
    today = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
    current_streak = 0
    for d in reversed(active_dates):
        expected = today - pd.Timedelta(days=current_streak)
        if d == expected:
            current_streak += 1
        else:
            break

    return {
        "longest_streak": longest,
        "current_streak": current_streak,
        "best_streak_start": str(best_start.date()),
        "best_streak_end": str(best_end.date()),
    }


def get_monthly_breakdown(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Returns total minutes listened per month for a given year.

    Args:
        df: Cleaned streaming DataFrame.
        year: Year to filter by.

    Returns:
        DataFrame with month and total minutes, ordered Jan-Dec.
    """
    month_order = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    monthly = (
        df[df["year"] == year]
        .groupby("month_name")["minutes_played"]
        .sum()
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )

    monthly["month_name"] = pd.Categorical(monthly["month_name"], categories=month_order, ordered=True)
    return monthly.sort_values("month_name").reset_index(drop=True)


def get_artist_loyalty_timeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the top artist per year based on total minutes played.
    Shows how listening taste evolved year over year.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        DataFrame with year, top artist, and minutes played.
    """
    return (
        df.groupby(["year", "artist"])["minutes_played"]
        .sum()
        .reset_index()
        .sort_values(["year", "minutes_played"], ascending=[True, False])
        .groupby("year")
        .first()
        .reset_index()
        .rename(columns={"minutes_played": "total_minutes"})
    )


def get_biggest_listening_day(df: pd.DataFrame) -> dict:
    """
    Finds the single day with the most listening time and
    returns what was playing that day.

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary with date, total minutes, and top tracks that day.
    """
    daily_totals = df.groupby("date")["minutes_played"].sum()
    best_date = daily_totals.idxmax()
    best_minutes = round(daily_totals.max(), 1)

    top_tracks_that_day = (
        df[df["date"] == best_date]
        .groupby("track")["minutes_played"]
        .sum()
        .sort_values(ascending=False)
        .head(3)
        .reset_index()
    )

    return {
        "date": str(best_date),
        "total_minutes": best_minutes,
        "total_hours": round(best_minutes / 60, 1),
        "top_tracks": top_tracks_that_day["track"].tolist(),
        "total_streams": int(df[df["date"] == best_date]["track"].count()),
    }

def analyze_time_of_day_listening(df: pd.DataFrame) -> dict:
    """
    Segments listening into time-of-day buckets and runs a one-way ANOVA
    to test whether listening volume differs significantly across periods.

    Buckets:
        Morning   — 06:00 to 11:59
        Afternoon — 12:00 to 17:59
        Evening   — 18:00 to 21:59
        Night     — 22:00 to 05:59

    Args:
        df: Cleaned streaming DataFrame.

    Returns:
        Dictionary with per-period averages, ANOVA result, and plain English summary.
    """
    from scipy import stats

    def assign_period(hour: int) -> str:
        if 6 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 18:
            return "Afternoon"
        elif 18 <= hour < 22:
            return "Evening"
        else:
            return "Night"

    df = df.copy()
    df["period"] = df["hour"].apply(assign_period)

    # Daily minutes per period
    daily_period = (
        df.groupby(["date", "period"])["minutes_played"]
        .sum()
        .reset_index()
    )

    period_order = ["Morning", "Afternoon", "Evening", "Night"]
    period_groups = {
        p: daily_period[daily_period["period"] == p]["minutes_played"]
        for p in period_order
    }

    period_avgs = {
        p: round(group.mean(), 1)
        for p, group in period_groups.items()
        if len(group) > 0
    }

    # One-way ANOVA across all four groups — requires at least 2 groups with data
    alpha = 0.05
    groups_for_anova = [g for g in period_groups.values() if len(g) > 1]

    if len(groups_for_anova) < 2:
        f_stat, p_value, significant = None, None, False
    else:
        f_stat, p_value = stats.f_oneway(*groups_for_anova)
        f_stat = round(f_stat, 4)
        p_value = round(p_value, 4)
        significant = p_value < alpha

    dominant_period = max(period_avgs, key=period_avgs.get)
    dominant_avg = period_avgs[dominant_period]
    alpha = 0.05
    significant = p_value < alpha

    period_emojis = {
        "Morning": "🌅",
        "Afternoon": "☀️",
        "Evening": "🌆",
        "Night": "🌙",
    }

    return {
        "period_avgs": period_avgs,
        "dominant_period": dominant_period,
        "dominant_emoji": period_emojis.get(dominant_period, ""),
        "f_statistic": f_stat,
        "p_value": p_value,
        "significant": significant,
        "period_order": period_order,
        "interpretation": (
            f"You are most active during the {dominant_period.lower()}, "
            f"averaging {dominant_avg} minutes per day during that period. "
            + (
                "This pattern is statistically significant — it reflects a genuine habit, not just random variation."
                if significant else
                "However, the difference across periods is not statistically significant, "
                "meaning your listening is fairly spread throughout the day."
            )
        ),
    }