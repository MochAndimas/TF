"""Shared endpoint helpers for authorization, validation, and error mapping."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import date
from typing import NoReturn

from fastapi import Depends, HTTPException, status

from app.db.models.user import TfUser
from app.schemas.responses import AnalyticsResponse
from app.utils.user_utils import get_current_user, require_roles


def require_roles_dep(*allowed_roles: str):
    """Create a dependency that enforces one of the allowed roles."""

    async def _dependency(current_user: TfUser = Depends(get_current_user)) -> TfUser:
        try:
            require_roles(current_user, *allowed_roles)
        except PermissionError as error:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=str(error),
            ) from error
        return current_user

    return _dependency


def raise_forbidden(error: PermissionError) -> NoReturn:
    """Map domain-level permission errors into standardized HTTP 403."""
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=str(error),
    ) from error


def raise_bad_request(error: ValueError) -> NoReturn:
    """Map domain-level validation errors into standardized HTTP 400."""
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=str(error),
    ) from error


def validate_date_range(
    start_date: date,
    end_date: date,
    *,
    start_name: str = "start_date",
    end_name: str = "end_date",
) -> None:
    """Validate inclusive date-range order for query/form payloads."""
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{start_name} cannot be after {end_name}.",
        )


async def build_analytics_response(
    *,
    loader: Callable[[], Awaitable[dict[str, object]]],
    success_message: str,
    logger: logging.Logger,
    failure_log_message: str,
    failure_detail_message: str,
) -> AnalyticsResponse:
    """Run one analytics payload loader with centralized exception mapping."""
    try:
        data = await loader()
        return AnalyticsResponse(
            success=True,
            message=success_message,
            data=data,
        )
    except HTTPException:
        raise
    except ValueError as error:
        raise_bad_request(error)
    except Exception:
        logger.exception(failure_log_message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=failure_detail_message,
        )
