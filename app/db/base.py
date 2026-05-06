"""Base module.

This module is part of `app.db` and contains runtime logic used by the
Traders Family application.
"""

from sqlalchemy.orm import declarative_base


# SQLite declarative base
SqliteBase = declarative_base()
