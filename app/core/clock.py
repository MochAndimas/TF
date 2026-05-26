"""Centralized clock helpers for application timestamps."""

from __future__ import annotations

from datetime import datetime


def now() -> datetime:
    """Return the current wall-clock timestamp used across service layers.

    This preserves existing naive ``datetime`` behavior while making timestamp
    sourcing consistent and easy to evolve in one place later.
    """
    return datetime.now()
