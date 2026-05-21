"""Role-based access-control helpers."""

from __future__ import annotations

from typing import Final

from app.db.models.user import TfUser

LEGACY_ROLE_ALIASES: Final[dict[str, str]] = {
    "admin": "analyst",
}

ALLOWED_ROLES: Final[set[str]] = {"superadmin", "analyst", "digital_marketing", "finance", "sales", *LEGACY_ROLE_ALIASES}
ROLE_CREATION_POLICY: Final[dict[str, set[str]]] = {
    "superadmin": ALLOWED_ROLES,
}

ANALYTICS_ROLES: Final[tuple[str, ...]] = ("superadmin", "analyst", "admin", "digital_marketing")
FINANCE_ANALYTICS_ROLES: Final[tuple[str, ...]] = ("superadmin", "analyst", "admin", "finance")


def validate_role_assignment(actor_role: str, target_role: str) -> None:
    """Validate whether the authenticated actor may assign the requested role."""
    normalized_actor_role = normalize_role(actor_role)
    normalized_target_role = normalize_role(target_role)

    if normalized_target_role not in ALLOWED_ROLES:
        raise ValueError("Invalid role selected.")

    allowed_roles = ROLE_CREATION_POLICY.get(normalized_actor_role, set())
    if normalized_target_role not in allowed_roles:
        raise PermissionError("Not authorized to assign the requested role.")


def require_roles(current_user: TfUser, *allowed_roles: str) -> None:
    """Enforce that a user belongs to one of the allowed RBAC roles."""
    normalized_role = normalize_role(current_user.role)
    normalized_allowed = {normalize_role(role) for role in allowed_roles}
    if normalized_role not in normalized_allowed:
        raise PermissionError("Not authorized to access this resource.")


def normalize_role(role: str | None) -> str:
    """Normalize stored role codes and keep legacy aliases compatible."""
    normalized = (role or "").strip().lower()
    return LEGACY_ROLE_ALIASES.get(normalized, normalized)
