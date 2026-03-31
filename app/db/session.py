"""Database engine and async-session management utilities."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, StaticPool

from app.core.config import settings


def _is_sqlite_memory_url(db_url: str) -> bool:
    """Return whether a SQLAlchemy URL points to an in-memory SQLite database."""
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        return False

    database = (url.database or "").strip()
    return database in {"", ":memory:"} or "mode=memory" in database


def _sqlite_file_path(db_url: str) -> str | None:
    """Resolve SQLite file path from SQLAlchemy URL when applicable."""
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite" or _is_sqlite_memory_url(db_url):
        return None

    database = (url.database or "").strip()
    return str(Path(database).expanduser())


def _configure_sqlite_engine(engine: AsyncEngine, *, memory_db: bool) -> None:
    """Attach SQLite pragmas tuned for single-node file-backed operation."""

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(f"PRAGMA busy_timeout={settings.SQLITE_BUSY_TIMEOUT_MS}")
            if not memory_db:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA temp_store=MEMORY")
        finally:
            cursor.close()


def create_engine(db_url: str) -> AsyncEngine:
    """Create async SQLAlchemy engine for SQLite-backed application storage.

    Args:
        db_url (str): SQLAlchemy async connection URL.

    Returns:
        AsyncEngine: Configured asynchronous engine instance.
    """
    memory_db = _is_sqlite_memory_url(db_url)
    sqlite_file_path = _sqlite_file_path(db_url)

    engine_kwargs: dict[str, object] = {
        "echo": False,
        "pool_pre_ping": True,
    }
    if memory_db:
        engine_kwargs["poolclass"] = StaticPool
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    elif sqlite_file_path is not None:
        engine_kwargs["poolclass"] = NullPool
        engine_kwargs["connect_args"] = {
            "check_same_thread": False,
            "timeout": settings.SQLITE_BUSY_TIMEOUT_MS / 1000,
        }

    engine = create_async_engine(db_url, **engine_kwargs)
    if memory_db or sqlite_file_path is not None:
        _configure_sqlite_engine(engine, memory_db=memory_db)
    return engine


def create_session_factory(engine: AsyncEngine) -> sessionmaker:
    """Create SQLAlchemy async session factory bound to a given engine.

    Args:
        engine (AsyncEngine): Active SQLAlchemy engine.

    Returns:
        sessionmaker: Factory producing ``AsyncSession`` instances.
    """
    return sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )


sqlite_engine = create_engine(settings.DB_URL)
sqlite_async_session = create_session_factory(sqlite_engine)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield a scoped async database session for request handlers.

    Yields:
        AsyncSession: Active SQLAlchemy async session.
    """
    async with sqlite_async_session() as session:
        yield session
