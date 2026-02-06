from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float
from app.db.base import SqliteBase


class TfUser(SqliteBase):
    """
    Docstring for TfUser
    """
    __tablename__ = "tf_user"

    user_id = Column("user_id", Integer, primary_key=True, autoincrement=True)
    fullname = Column("fullname", String,)
    email = Column("email", String, nullable=False)
    role = Column("role", String, nullable=False)
    password = Column("password", String, nullable=False)
    created_at = Column("created_at", DateTime, nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)
    deleted_at = Column("deleted_at", DateTime, nullable=False)


class UserToken(SqliteBase):
    """
    Docstring for UserToken
    """
    __tablename__ = "user_token"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    session_id = Column("session_id", Integer, nullable=False)
    user_id = Column("user_id", Integer, nullable=False)
    page = Column("page", String, nullable=False)
    logged_in = Column("logged_in", Boolean, nullable=False)
    role = Column("role",String, nullable=False)
    expiry = Column("expiry", DateTime, nullable=False)
    access_token = Column("access_token", String, nullable=False)
    refresh_token = Column("refresh_token", String, nullable=False)
    is_revoked = Column("is_revoked", Boolean, nullable=False)
    created_at = Column("created_at",DateTime, nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)
    deleted_at = Column("deleted_at", DateTime, nullable=True)
