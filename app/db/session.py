"""Database engine and async-session management utilities."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import settings


def create_engine(db_url: str) -> AsyncEngine:
    """Create async SQLAlchemy engine for SQLite-backed application storage.

    Args:
        db_url (str): SQLAlchemy async connection URL.

    Returns:
        AsyncEngine: Configured asynchronous engine instance.
    """
    return create_async_engine(
        db_url,
        echo=False,
        poolclass=StaticPool,
        pool_pre_ping=True,
    )


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

