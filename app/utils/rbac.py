"""Role-based access-control helpers."""

from __future__ import annotations

from typing import Final

from app.db.models.user import TfUser

ALLOWED_ROLES: Final[set[str]] = {"superadmin", "admin", "digital_marketing", "sales", "finance"}
ROLE_CREATION_POLICY: Final[dict[str, set[str]]] = {
    "superadmin": ALLOWED_ROLES,
    "admin": {"digital_marketing", "sales"},
}


def validate_role_assignment(actor_role: str, target_role: str) -> None:
    """Validate whether the authenticated actor may assign the requested role."""
    normalized_actor_role = actor_role.strip().lower()
    normalized_target_role = target_role.strip().lower()

    if normalized_target_role not in ALLOWED_ROLES:
        raise ValueError("Invalid role selected.")

    allowed_roles = ROLE_CREATION_POLICY.get(normalized_actor_role, set())
    if normalized_target_role not in allowed_roles:
        raise PermissionError("Not authorized to assign the requested role.")


def require_roles(current_user: TfUser, *allowed_roles: str) -> None:
    """Enforce that a user belongs to one of the allowed RBAC roles."""
    normalized_role = (current_user.role or "").strip().lower()
    normalized_allowed = {role.strip().lower() for role in allowed_roles}
    if normalized_role not in normalized_allowed:
        raise PermissionError("Not authorized to access this resource.")
