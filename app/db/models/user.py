from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float
from app.db.base import SqliteBase


class TfUser(SqliteBase):
    """SQLAlchemy model for application users.

    Attributes:
        user_id (str): Primary key user identifier (UUID string).
        fullname (str): User full name.
        email (str): User email address.
        role (str): Role assigned to the user.
        password (str): Hashed password value.
        created_at (DateTime): Creation timestamp.
        updated_at (DateTime): Last modification timestamp.
        deleted_at (DateTime | None): Soft-delete timestamp.
    """
    __tablename__ = "tf_user"

    user_id = Column("user_id", String, primary_key=True)
    fullname = Column("fullname", String, nullable= False)
    email = Column("email", String, nullable=False)
    role = Column("role", String, nullable=False)
    password = Column("password", String, nullable=False)
    created_at = Column("created_at", DateTime, nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)
    deleted_at = Column("deleted_at", DateTime, nullable=True)


class UserToken(SqliteBase):
    """SQLAlchemy model for persisted login/session tokens.

    Attributes:
        id (int): Primary key.
        session_id (str): Session identifier (UUID string).
        user_id (str): Related user identifier.
        page (str): Last visited page marker.
        logged_in (bool): Current login state.
        role (str): Role cached at login time.
        expiry (DateTime): Token/session expiry timestamp.
        access_token (str): Current access token.
        refresh_token (str): Current refresh token.
        is_revoked (bool): Revocation state.
        created_at (DateTime): Creation timestamp.
        updated_at (DateTime): Last update timestamp.
        deleted_at (DateTime | None): Soft-delete timestamp.
    """
    __tablename__ = "user_token"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    session_id = Column("session_id", String, nullable=False)
    user_id = Column("user_id", String, nullable=False)
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


class LogData(SqliteBase):
    """SQLAlchemy model for HTTP request/response logs.

    Attributes:
        id (int): Primary key.
        url (str): Requested URL.
        method (str): HTTP method.
        time (float): Request processing duration in seconds.
        status (int): HTTP response status code.
        response (JSON): Response body snapshot.
        created_at (DateTime): Log creation timestamp.
    """
    __tablename__ = "log_data"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    url = Column("url", String, nullable=False)
    method = Column("method", String, nullable=False)
    time = Column("time", Float, nullable=False)
    status = Column("status", Integer, nullable=False)
    response = Column("response", JSON, nullable=False)
    created_at = Column(DateTime, nullable=False)
