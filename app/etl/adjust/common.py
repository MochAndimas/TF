"""Shared callback decoding, identity and timestamp normalization."""
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


INSTALL_DEVICE_FIELDS = ('gps_adid', 'idfa', 'city', 'country', 'device_name', 'os_version', 'isp', 'mcc', 'mnc')


PROFILE_FIELDS = ("username", "email", "nomor_telepon")


def optional_text(value):
    text = str(value).strip() if value is not None else ""
    if "{" in text or "}" in text or re.fullmatch(r"\[[A-Za-z_][A-Za-z0-9_]*\]", text):
        return None
    return text or None


def required_text(payload, key):
    value = optional_text(payload.get(key))
    if value is None:
        raise ValueError(f"Missing or unresolved {key}")
    return value


def normalize_identity(payload):
    environment = required_text(payload, "environment").lower()
    if environment not in {"sandbox", "production"}:
        raise ValueError("Invalid environment")
    return dict(app_token=required_text(payload, "app_token"), environment=environment,
                adid=required_text(payload, "adid"))


def unix_timestamp(payload, key, *, required=False):
    value = optional_text(payload.get(key))
    if value is None and not required:
        return None
    try:
        seconds = Decimal(value or "")
        if not seconds.is_finite() or seconds <= 0:
            raise ValueError("Invalid timestamp")
        return datetime.fromtimestamp(float(seconds), timezone.utc).replace(tzinfo=None)
    except (InvalidOperation, OverflowError, OSError, ValueError) as error:
        raise ValueError(f"{key} must be a positive Unix timestamp in seconds") from error


def optional_integer(payload, key):
    value = optional_text(payload.get(key))
    if value is None:
        return None
    try:
        number = Decimal(value)
        if not number.is_finite() or number < 0 or number != number.to_integral_value() or number > 9223372036854775807:
            raise ValueError("Invalid integer")
        return int(number)
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{key} must be a nonnegative integer") from error


def reporting_date(timestamp):
    return timestamp.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Asia/Jakarta")).date()


def normalize_context(payload):
    row = {}
    for field in ("os_name", "network_name", "campaign_name", "adgroup_name", "creative_name", "tracker_token", "tracker_name"):
        value = payload.get(field)
        text = optional_text(value) or ""
        # Adjust export strings can retain form encoding; keep raw payload intact.
        if field in {"network_name", "campaign_name", "adgroup_name", "creative_name", "tracker_name"}:
            if re.search(r"%[0-9a-fA-F]{2}", text) or (" " not in text and re.search(r"\w\+\w", text)):
                text = unquote_plus(text)
        row[field] = optional_text(text)
    for field in INSTALL_DEVICE_FIELDS:
        value = payload.get(field)
        text = str(value).strip() if value is not None else ""
        if "{" in text or "}" in text:
            text = ""
        if field in {"city", "device_name", "isp"} and (
            re.search(r"%[0-9a-fA-F]{2}", text) or (" " not in text and re.search(r"\w\+\w", text))
        ):
            text = unquote_plus(text)
        row[field] = optional_text(text)
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
