"""Pure install normalization; timestamps are UTC, reporting dates are WIB."""
import hashlib
import json
import re
from urllib.parse import unquote_plus
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo


def payload_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def normalize_source_name(network_name: str | None) -> str | None:
    """Map provider labels to short names; retain other providers as a short slug."""
    value = (network_name or "").strip().lower()
    if not value:
        return None
    aliases = {
        "google": ("google", "adwords"),
        "facebook": ("facebook", "meta", "instagram", "fb"),
        "tiktok": ("tiktok", "tik tok"),
        "apple": ("apple",),
        "organic": ("organic",),
        "unknown": ("unknown", "unattributed"),
    }
    for source, prefixes in aliases.items():
        if any(value == prefix or value.startswith(prefix + " ") for prefix in prefixes):
            return source
    first_word = value.split()[0]
    return re.sub(r"[^a-z0-9]+", "_", first_word).strip("_") or "unknown"


def normalize_install(payload: dict, raw_id: int) -> dict:
    def required(key):
        value = str(payload.get(key) or "").strip()
        if not value or "{" in value or "}" in value:
            raise ValueError(f"Missing or unresolved {key}")
        return value

    if required("activity_kind").lower() != "install":
        raise ValueError("Only install activity is supported")
    environment = required("environment").lower()
    if environment not in {"sandbox", "production"}:
        raise ValueError("Invalid environment")
    app_token, adid = required("app_token"), required("adid")
    try:
        seconds = Decimal(required("installed_at"))
        if not seconds.is_finite() or seconds <= 0:
            raise ValueError("Invalid installed_at")
        installed = datetime.fromtimestamp(float(seconds), timezone.utc)
    except (InvalidOperation, OverflowError, OSError, ValueError) as error:
        raise ValueError("installed_at must be a positive Unix timestamp in seconds") from error
    key = payload_hash({"app_token": app_token, "environment": environment, "adid": adid, "installed_at": installed.isoformat()})
    row = dict(install_key=key, app_token=app_token, environment=environment, adid=adid,
               installed_at=installed.replace(tzinfo=None), date=installed.astimezone(ZoneInfo("Asia/Jakarta")).date(), raw_id=raw_id)
    for field in ("os_name", "network_name", "campaign_name", "adgroup_name", "creative_name", "tracker_token", "tracker_name"):
        value = payload.get(field)
        text = str(value).strip() if value is not None else ""
        # Adjust export strings can retain form encoding; keep raw payload intact.
        if field in {"network_name", "campaign_name", "adgroup_name", "creative_name", "tracker_name"}:
            if re.search(r"%[0-9a-fA-F]{2}", text) or (" " not in text and re.search(r"\w\+\w", text)):
                text = unquote_plus(text)
        row[field] = text or None
    campaign_id = payload.get("campaign_id")
    campaign_id = str(campaign_id).strip() if campaign_id is not None else ""
    row["campaign_id"] = (
        campaign_id if campaign_id and "{" not in campaign_id and "}" not in campaign_id else None
    )
    row["source_name"] = normalize_source_name(row.pop("network_name"))
    if row["campaign_id"] is None and row["source_name"] == "google":
        match = re.search(r"\((\d+)\)\s*$", row["campaign_name"] or "")
        if match:
            row["campaign_id"] = match.group(1)
    # Remove only the suffix known to be the campaign ID, not arbitrary parentheses.
    if row["campaign_id"] and row["campaign_name"]:
        suffix = re.search(r"\s*\((\d+)\)\s*$", row["campaign_name"])
        if suffix and suffix.group(1) == row["campaign_id"]:
            row["campaign_name"] = row["campaign_name"][:suffix.start()].strip() or None
    return row
