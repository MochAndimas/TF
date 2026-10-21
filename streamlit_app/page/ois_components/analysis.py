"""Pure daily-fact analysis; period unique-user rates are never fabricated."""
import pandas as pd
from app.etl.ga4.config import KEY_COLUMNS, METRICS

SUM_FIELDS = ["new_users", "active_users", "total_users", "engaged_sessions",
              "user_engagement_duration", "event_count", "key_events"]
LABELS = {
    "date": "Date", "platform": "Platform", "first_user_source": "Source",
    "first_user_medium": "Medium", "first_user_campaign_name": "Campaign",
    "new_users": "New users", "active_users": "Active user-days",
    "total_users": "Total user-days", "engaged_sessions": "Engaged sessions",
    "user_engagement_duration": "Engagement time (s)", "event_count": "Events",
    "key_events": "Key events", "user_key_event_rate": "User key event rate",
    "engagement_seconds_per_active_user_day": "Engagement seconds / active user-day",
    "sessions_per_active_user_day": "Engaged sessions / active user-day",
    "pull_date": "Last synced",
}


def frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=[*KEY_COLUMNS, *METRICS.values(), "pull_date"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def aggregate(df: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[*dimensions, *SUM_FIELDS,
                                     "engagement_seconds_per_active_user_day", "sessions_per_active_user_day"])
    grouped = df.groupby(dimensions, as_index=False, dropna=False)[SUM_FIELDS].sum()
    denominator = grouped["active_users"].replace(0, float("nan"))
    grouped["engagement_seconds_per_active_user_day"] = grouped["user_engagement_duration"] / denominator
    grouped["sessions_per_active_user_day"] = grouped["engaged_sessions"] / denominator
    return grouped


def totals(df: pd.DataFrame) -> dict:
    return {field: float(df[field].sum()) for field in SUM_FIELDS}


def growth(current: float, previous: float) -> str | None:
    return f"{(current / previous - 1) * 100:+.1f}%" if previous else None


def insights(df: pd.DataFrame, start, end) -> list[str]:
    """Describe observed contributions and peaks without claiming causation."""
    if df.empty:
        return []
    messages = []
    sources = aggregate(df, ["first_user_source"])
    total_new = sources.new_users.sum()
    if total_new:
        leader = sources.loc[sources.new_users.idxmax()]
        messages.append(f"**{leader.first_user_source}** contributes **{leader.new_users:,.0f} new users** "
                        f"({leader.new_users / total_new:.1%} of the selected mix).")
    daily = aggregate(df, ["date"])
    if total_new:
        peak = daily.loc[daily.new_users.idxmax()]
        messages.append(
            f"Acquisition peaked on **{peak.date:%d %b %Y}** with **{peak.new_users:,.0f} new users**.")
    campaigns = aggregate(df, ["first_user_campaign_name"])
    named = campaigns[campaigns.first_user_campaign_name != "(not set)"]
    if not named.empty and named.new_users.sum():
        leader = named.loc[named.new_users.idxmax()]
        messages.append(
            f"**{leader.first_user_campaign_name}** leads named campaigns with **{leader.new_users:,.0f} new users**.")
    unassigned = df.loc[df.first_user_campaign_name == "(not set)", "new_users"].sum()
    if total_new and unassigned:
        messages.append(
            f"**{unassigned / total_new:.1%}** of new users have no campaign name; campaign comparisons cover the tagged portion.")
    days = (pd.Timestamp(end) - pd.Timestamp(start)).days + 1
    observed = df.date.nunique()
    if observed < days:
        messages.append(
            f"Records are available on **{observed} of {days} days**. Gaps remain blank in the trend charts.")
    return messages
