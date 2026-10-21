"""Daily first-user acquisition facts; user counts are not additive over dates."""
from sqlalchemy import CheckConstraint, Column, Date, Float, Integer, String, UniqueConstraint
from app.db.base import SqliteBase


class Ga4UserAcquisition(SqliteBase):
    __tablename__ = "ga4_user_acquisition"
    __table_args__ = (
        UniqueConstraint("date", "platform", "first_user_source", "first_user_medium",
                         "first_user_campaign_name", name="uq_ga4_user_acquisition_grain"),
        CheckConstraint("first_user_source IN ('ois_id', 'ois.id')", name="ck_ga4_acquisition_source"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    platform = Column(String, nullable=False)
    first_user_source = Column(String, nullable=False, index=True)
    first_user_medium = Column(String, nullable=False)
    first_user_campaign_name = Column(String, nullable=False)
    new_users = Column(Integer, nullable=False)
    active_users = Column(Integer, nullable=False)
    total_users = Column(Integer, nullable=False)
    engaged_sessions = Column(Integer, nullable=False)
    user_engagement_duration = Column(Float, nullable=False)
    event_count = Column(Integer, nullable=False)
    key_events = Column(Float, nullable=False)
    user_key_event_rate = Column(Float, nullable=False)
    pull_date = Column(Date, nullable=False)

    @property
    def average_engagement_time_per_active_user(self):
        return self.user_engagement_duration / self.active_users if self.active_users else 0.0

    @property
    def engaged_sessions_per_active_user(self):
        return self.engaged_sessions / self.active_users if self.active_users else 0.0
