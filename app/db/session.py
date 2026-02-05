from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app.core.config import settings


# Create sqlite engine
sqlite_engine = create_async_engine(
    settings.DB_URL,
    echo=False,
    poolclass=StaticPool,
    pool_pre_ping=True
)

# create sqlite async session
sqlite_async_session = sessionmaker(
    bind=sqlite_engine,
    expire_on_commit=False,
    class_=AsyncSession
)

async def get_db():
    """
    Docstring for get_db

    Dependency function to provide an async sqlite session per request.
    """
    async with sqlite_async_session() as session:
        try:
            yield session
        finally:
            await session.close()

