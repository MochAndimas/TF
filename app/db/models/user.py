from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float
from app.db.base import SqliteBase


class UserToken(SqliteBase):
    """
    Docstring for UserToken
    """
    __tablename__ = "user_token"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    page = Column(String, nullable=False)
    logged_in = Column(Boolean, nullable=False)
    role = Column(String, nullable=False)
    expiry = Column(DateTime, nullable=False)
    access_token = Column(String, nullable=False)
    refresh_token = Column(String, nullable=False)
    is_revoked = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
