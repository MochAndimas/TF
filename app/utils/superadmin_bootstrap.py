"""Bootstrap helper for one-time superadmin account provisioning."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import select

from app.core.config import settings
from app.core.security import pwd_context, validate_password_policy
from app.db.models.user import TfUser
from app.db.session import sqlite_async_session


class SuperadminBootstrapService:
    """Provision the initial superadmin account when bootstrap mode is enabled."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    async def ensure_bootstrap_superadmin(self) -> None:
        """Create the initial superadmin account if bootstrap preconditions are met."""
        settings.validate_bootstrap_config()
        if not settings.BOOTSTRAP_SUPERADMIN:
            return

        bootstrap_email = str(settings.INITIAL_SUPERADMIN_EMAIL).lower().strip()
        validate_password_policy(str(settings.INITIAL_SUPERADMIN_PASSWORD))
        now = datetime.now()

        async with sqlite_async_session() as session:
            existing_user_result = await session.execute(
                select(TfUser).where(
                    TfUser.email == bootstrap_email,
                    TfUser.deleted_at.is_(None),
                )
            )
            existing_user = existing_user_result.scalars().first()
            if existing_user is not None:
                self._logger.info(
                    "Bootstrap superadmin skipped: account already exists for %s",
                    bootstrap_email,
                )
                return

            active_users_result = await session.execute(
                select(TfUser.user_id).where(TfUser.deleted_at.is_(None))
            )
            if active_users_result.first() is not None:
                self._logger.warning(
                    "Bootstrap superadmin skipped: active users already exist. "
                    "Disable BOOTSTRAP_SUPERADMIN."
                )
                return

            session.add(
                TfUser(
                    user_id=str(uuid.uuid4()),
                    fullname=str(settings.INITIAL_SUPERADMIN_NAME).strip(),
                    email=bootstrap_email,
                    role="superadmin",
                    password=pwd_context.hash(str(settings.INITIAL_SUPERADMIN_PASSWORD)),
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                )
            )
            await session.commit()

        self._logger.warning(
            "Bootstrap superadmin created for %s. Set BOOTSTRAP_SUPERADMIN=false after first deploy.",
            bootstrap_email,
        )
