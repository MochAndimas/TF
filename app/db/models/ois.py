"""User-level OIS registration records, separate from GA4 aggregate facts."""
from sqlalchemy import Boolean, Column, Date, DateTime, Float, String
from app.db.base import SqliteBase


class OisRegistration(SqliteBase):
    __tablename__ = "ois_registrations"

    tf_id = Column(String, primary_key=True)
    registration_date = Column(Date, nullable=False, index=True)
    tgl_regis = Column(DateTime, nullable=False)
    email = Column(String)
    phone = Column(String)
    username = Column(String)
    utm_source = Column(String, nullable=False, index=True)
    utm_medium = Column(String)
    utm_campaign = Column(String)
    user_status = Column(String)
    assign_date = Column(DateTime)
    analyst = Column(String)
    first_subs = Column(DateTime)
    first_subs_idr = Column(Float)
    time_to_closing = Column(String)
    cabang = Column(String)
    pool = Column(Boolean)
    status = Column(String)
    first_assign_date = Column(DateTime)
    last_subs = Column(DateTime)
    last_subs_nominal_idr = Column(Float)
    last_activity = Column(DateTime)
    pull_date = Column(Date, nullable=False)
