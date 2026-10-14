"""Install transformation and idempotent loading."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from sqlalchemy import case, func
from sqlalchemy.dialects.sqlite import insert
from app.db.models.adjust import AdjustInstall
from app.etl.adjust.common import INSTALL_DEVICE_FIELDS, normalize_context, payload_hash


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
    row.update(normalize_context(payload))
    return row


async def load_install(session, fact):
    statement = insert(AdjustInstall).values(**fact)
    updates = {"campaign_id": func.coalesce(AdjustInstall.campaign_id, statement.excluded.campaign_id)}
    for field in INSTALL_DEVICE_FIELDS:
        updates[field] = func.coalesce(getattr(AdjustInstall, field), getattr(statement.excluded, field))
    for field in ("source_name", "campaign_name", "adgroup_name", "creative_name", "tracker_name"):
        updates[field] = case((AdjustInstall.raw_id == fact["raw_id"], getattr(statement.excluded, field)), else_=getattr(AdjustInstall, field))
    await session.execute(statement.on_conflict_do_update(index_elements=["install_key"], set_=updates))
