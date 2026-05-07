"""Pure helper functions for external-source extraction."""

from __future__ import annotations

import json
from pathlib import Path

from decouple import config


def load_service_account_info(env_key: str) -> dict:
    """Load service-account credentials from env JSON or a JSON file path."""
    raw_value = config(env_key, default="", cast=str).strip()
    if not raw_value:
        raise ValueError(f"{env_key} is required for service-account auth.")

    normalized = raw_value.strip().strip("'").strip('"')
    if normalized.startswith("{"):
        try:
            return json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{env_key} JSON string is invalid. Make sure it is valid single-line JSON."
            ) from exc

    path_candidate = Path(normalized)
    try:
        if path_candidate.exists():
            return json.loads(path_candidate.read_text(encoding="utf-8"))
    except OSError:
        # Some OSes raise "file name too long" when a raw JSON blob is probed as a path.
        pass

    try:
        return json.loads(normalized)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{env_key} must be either a single-line JSON string or a path to a service-account JSON file."
        ) from exc


def normalize_customer_id(value: str | None) -> str | None:
    """Normalize a Google Ads customer identifier by removing separators."""
    normalized = str(value or "").strip().replace("-", "")
    return normalized or None


def normalize_meta_ad_account_id(value: str | None) -> str | None:
    """Normalize Meta ad account ID into ``act_<id>`` form."""
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.startswith("act_"):
        return normalized
    return f"act_{normalized}"


def extract_meta_leads(actions: list[dict] | None) -> int:
    """Extract a best-effort lead total from Meta Insights actions payload."""
    if not actions:
        return 0

    lead_total = 0.0
    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = str(action.get("action_type") or "").strip().lower()
        if not action_type or "lead" not in action_type:
            continue
        try:
            lead_total += float(action.get("value") or 0)
        except (TypeError, ValueError):
            continue
    return int(round(lead_total))
