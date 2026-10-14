"""Activity dispatch shared by ingestion and the single sequential ETL run."""
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from sqlalchemy import func, select
from app.db.models.adjust import AdjustInstall, AdjustLifecycle, AdjustAttributionHistory, AdjustEngagement
from app.db.models.adjust import AdjustCost
from app.etl.adjust.handlers.cost import COST_KINDS, load_cost, normalize_cost
from app.etl.adjust.handlers.install import load_install, normalize_install
from app.etl.adjust.handlers.lifecycle import LIFECYCLE_TIMESTAMPS, load_lifecycle, normalize_lifecycle

from app.etl.adjust.handlers.attribution import ATTRIBUTION_TIMESTAMPS, load_attribution, normalize_attribution

from app.etl.adjust.handlers.engagement import ENGAGEMENT_TIMESTAMPS, load_engagement, normalize_engagement

from app.etl.adjust.handlers.event import EVENT_MODELS, load_event, normalize_event


@dataclass(frozen=True)
class CallbackHandler:
    normalize: Callable[[dict, int], dict]
    load: Callable[..., Awaitable[None]]
    model: type | tuple[type, ...]


INSTALL_HANDLER = CallbackHandler(normalize_install, load_install, AdjustInstall)
LIFECYCLE_HANDLER = CallbackHandler(normalize_lifecycle, load_lifecycle, AdjustLifecycle)
ATTRIBUTION_HANDLER = CallbackHandler(normalize_attribution, load_attribution, AdjustAttributionHistory)
ENGAGEMENT_HANDLER = CallbackHandler(normalize_engagement, load_engagement, AdjustEngagement)
EVENT_HANDLER = CallbackHandler(normalize_event, load_event, tuple(EVENT_MODELS.values()))
COST_HANDLER = CallbackHandler(normalize_cost, load_cost, AdjustCost)
CALLBACK_HANDLERS = {
    **{kind: COST_HANDLER for kind in COST_KINDS},
    "install": INSTALL_HANDLER,
    "event": EVENT_HANDLER,
    **{kind: ENGAGEMENT_HANDLER for kind in ENGAGEMENT_TIMESTAMPS},
    **{kind: LIFECYCLE_HANDLER for kind in LIFECYCLE_TIMESTAMPS},
    **{kind: ATTRIBUTION_HANDLER for kind in ATTRIBUTION_TIMESTAMPS},
}


async def count_callback_facts(session) -> int:
    """Total stored facts across supported activities, independent of receipt window."""
    total = 0
    models = [handler.model for kind, handler in CALLBACK_HANDLERS.items() if kind != "event"] + list(EVENT_MODELS.values())
    for model in dict.fromkeys(models):
        total += await session.scalar(select(func.count()).select_from(model)) or 0
    return total
