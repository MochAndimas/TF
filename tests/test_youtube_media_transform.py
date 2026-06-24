from app.etl.quality import validate_youtube_media_insight_dataframe
from app.etl.transform import parse_youtube_media_insight_dataframe


def _youtube_media_row(video_id: str, *, views: int, title: str = "Video") -> dict:
    return {
        "date": "2026-06-23",
        "video_id": video_id,
        "title": title,
        "published_at": "2026-06-23T08:00:00+00:00",
        "content_type": "video",
        "thumbnail_url": "https://example.com/thumb.jpg",
        "permalink": f"https://www.youtube.com/watch?v={video_id}",
        "views": views,
        "watch_hours": 1.5,
        "average_view_percentage": 44.2,
        "likes": 10,
        "comments": 2,
        "shares": 1,
        "subscribers_gained": 3,
    }


def test_youtube_media_transform_dedupes_video_id_before_validation() -> None:
    rows = [
        _youtube_media_row("abc123", views=100, title="Original"),
        _youtube_media_row("abc123", views=150, title="Latest"),
        _youtube_media_row("def456", views=75),
    ]

    df = parse_youtube_media_insight_dataframe(rows)

    assert len(df) == 2
    assert df["video_id"].tolist() == ["abc123", "def456"]
    assert df.loc[df["video_id"] == "abc123", "views"].item() == 150
    assert df.loc[df["video_id"] == "abc123", "title"].item() == "Latest"
    validate_youtube_media_insight_dataframe(df)
