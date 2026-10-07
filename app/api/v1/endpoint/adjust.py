"""Adjust callback delivery: authenticate, persist, acknowledge; no inline ETL."""
import hmac
import ipaddress
import json
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.db.models.adjust import AdjustCallbackRaw
from app.db.session import get_db
from app.etl.adjust.pipeline import utc_now
from app.etl.adjust.transform import payload_hash

router = APIRouter()
MAX_BODY_BYTES = 1_048_576


def _reject_nonfinite(value: str):
    """Reject nonstandard JSON constants rather than persisting invalid JSON."""
    raise ValueError(value)


@router.post("/api/adjust/callback", status_code=202)
async def receive_callback(request: Request, session: Annotated[AsyncSession, Depends(get_db)],
                           token: Annotated[str, Query()] = ""):
    # FastAPI has parsed the credential; redact it before Uvicorn access logging.
    request.scope["query_string"] = b""
    expected = settings.ADJUST_CALLBACK_SECRET
    if not expected:
        raise HTTPException(503, "Adjust callback ingestion is not configured")
    if not hmac.compare_digest(token.encode(), expected.encode()):
        raise HTTPException(403, "Invalid callback credential")
    if settings.ADJUST_CALLBACK_ALLOWED_CIDRS:
        try:
            peer = ipaddress.ip_address(request.client.host if request.client else "")
            allowed = any(peer in ipaddress.ip_network(cidr.strip()) for cidr in settings.ADJUST_CALLBACK_ALLOWED_CIDRS.split(","))
        except ValueError:
            raise HTTPException(503, "Invalid callback IP configuration") from None
        if not allowed:
            raise HTTPException(403, "Callback sender is not allowed")
    if request.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json":
        raise HTTPException(415, "Use application/json")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_BODY_BYTES:
            raise HTTPException(413, "Callback body exceeds 1 MiB")
    try:
        payload = json.loads(body, parse_constant=_reject_nonfinite)
        if not isinstance(payload, dict) or not payload or any(isinstance(v, (dict, list)) for v in payload.values()):
            raise ValueError("Expected a nonempty flat JSON object")
        digest = payload_hash(payload)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(400, "Expected a nonempty flat JSON object") from None
    if str(payload.get("app_token", "")) not in {item.strip() for item in settings.ADJUST_APP_TOKENS.split(",") if item.strip()}:
        raise HTTPException(400, "Unknown Adjust app_token")
    if payload.get("environment") != settings.ADJUST_CALLBACK_ENVIRONMENT:
        raise HTTPException(400, "Unexpected Adjust environment")
    if payload.get("activity_kind") != "install":
        raise HTTPException(400, "Only install callbacks are supported")
    try:
        await session.execute(insert(AdjustCallbackRaw).values(payload_hash=digest, payload=payload, received_at=utc_now(), status="pending", attempts=0).on_conflict_do_nothing(index_elements=["payload_hash"]))
        await session.commit()
    except Exception:
        await session.rollback()
        raise HTTPException(503, "Callback could not be persisted; retry delivery") from None
    return {"accepted": True}
