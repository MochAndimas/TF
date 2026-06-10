"""Account-management dialogs for Streamlit admin pages."""

from __future__ import annotations

import re

import httpx
import streamlit as st

from streamlit_app.functions.accounts import ROLE_OPTIONS, format_role_label, is_valid_email

PASSWORD_MIN_LENGTH = 12
PASSWORD_SPECIAL_PATTERN = re.compile(r"[^A-Za-z0-9]")


def _auth_headers(token: str) -> dict[str, str]:
    """Build the standard bearer-auth header set for backend API calls."""
    return {"Authorization": f"Bearer {token}"}


def _field_label(field_name: str) -> str:
    """Map API field names into friendlier UI labels."""
    return {
        "fullname": "Fullname",
        "email": "Email",
        "password": "Password",
        "confirm_password": "Confirm Password",
        "role": "Role",
    }.get(field_name, field_name.replace("_", " ").title())


def _format_error_detail(detail) -> str:
    """Convert backend validation payloads into concise user-friendly text."""
    if isinstance(detail, str):
        return detail

    if isinstance(detail, list):
        messages: list[str] = []
        for item in detail:
            if not isinstance(item, dict):
                continue
            location = item.get("loc", [])
            field_name = location[-1] if location else "field"
            field_label = _field_label(str(field_name))
            raw_message = str(item.get("msg") or "Invalid value.")
            normalized_message = raw_message[:1].lower() + raw_message[1:] if raw_message else "is invalid."
            messages.append(f"{field_label} {normalized_message}")
        if messages:
            return "\n".join(messages)

    if isinstance(detail, dict):
        return str(detail.get("message") or detail.get("detail") or "Something error, please try again!")

    return "Something error, please try again!"


def _password_requirement_results(password: str) -> list[tuple[str, bool]]:
    """Return password requirement labels and whether the password satisfies each one."""
    return [
        (f"Minimal {PASSWORD_MIN_LENGTH} karakter", len(password) >= PASSWORD_MIN_LENGTH),
        ("Ada huruf besar, contoh A", bool(re.search(r"[A-Z]", password))),
        ("Ada huruf kecil, contoh a", bool(re.search(r"[a-z]", password))),
        ("Ada angka, contoh 1", bool(re.search(r"\d", password))),
        ("Ada special character, contoh ! @ #", bool(PASSWORD_SPECIAL_PATTERN.search(password))),
        ("Minimal 6 karakter unik", len(set(password)) >= 6),
    ]


def _password_missing_requirements(password: str) -> list[str]:
    """List unsatisfied password requirements."""
    return [label for label, passed in _password_requirement_results(password) if not passed]


def _render_password_requirements(password: str, confirm_password: str) -> None:
    """Show a compact live checklist for account password requirements."""
    rows: list[str] = []
    for label, passed in _password_requirement_results(password):
        color = "#22c55e" if passed else "#ef4444"
        status = "OK" if passed else "Missing"
        rows.append(f"<div style='color:{color}; font-size:0.9rem;'>{status} - {label}</div>")

    match_passed = bool(password) and password == confirm_password
    if confirm_password:
        color = "#22c55e" if match_passed else "#ef4444"
        status = "OK" if match_passed else "Missing"
        rows.append(f"<div style='color:{color}; font-size:0.9rem;'>{status} - Confirm password sama dengan password</div>")

    st.markdown(
        "<div style='font-size:0.92rem; font-weight:700; margin-top:0.35rem; margin-bottom:0.2rem;'>Password requirements</div>"
        + "".join(rows),
        unsafe_allow_html=True,
    )


@st.dialog("Create Account")
def add_account_modal(host, token):
    """Render and process create-account modal form."""
    fullname = st.text_input("Fullname", key="create_account_fullname", width="stretch")
    email = st.text_input("Email", key="create_account_email", width="stretch")
    password = st.text_input("Password", type="password", key="create_account_password", width="stretch")
    confirm_password = st.text_input(
        "Confirm Password",
        type="password",
        key="create_account_confirm_password",
        width="stretch",
    )
    _render_password_requirements(password, confirm_password)

    role = st.selectbox("Role", list(ROLE_OPTIONS.keys()), key="create_account_role")
    submit = st.button("Create Account", type="primary", key="create_account_submit", width="stretch")

    if submit:
        missing_requirements = _password_missing_requirements(password)
        if not fullname.strip():
            st.warning("Fullname wajib diisi.")
        elif not is_valid_email(email):
            st.warning("Please input a real format email!")
        elif missing_requirements:
            st.warning("Password belum memenuhi:\n" + "\n".join(f"- {item}" for item in missing_requirements))
        elif password != confirm_password:
            st.warning("Confirm password harus sama dengan password.")
        else:
            with st.spinner("Creating account!"):
                try:
                    with httpx.Client(timeout=120) as client:
                        response = client.post(
                            f"{host}/api/register",
                            json={
                                "fullname": fullname,
                                "email": email,
                                "role": ROLE_OPTIONS[role],
                                "password": password,
                                "confirm_password": confirm_password,
                            },
                            headers=_auth_headers(token),
                        )
                        response.raise_for_status()
                        response_data = response.json()

                    if response_data.get("success"):
                        st.info("Successfully created an account!")
                        st.rerun()
                    st.error(response_data.get("detail") or "Unable to create account.")
                except httpx.HTTPStatusError as error:
                    detail = _format_error_detail(
                        error.response.json().get("detail", "Something error, please try again!")
                    )
                    st.error(f"Error creating account:\n{detail}")
                except Exception as error:
                    st.error(f"Error creating account: {error}")


@st.dialog("Manage Account")
def edit_account_modal(host, user, token):
    """Render and process account edit/delete modal actions."""
    with st.form("edit", border=False, clear_on_submit=True):
        fullname = st.text_input("Fullname", placeholder=user.fullname, width="stretch")
        email = st.text_input("Email", placeholder=user.email, width="stretch")
        role_options = {"": "", **ROLE_OPTIONS}
        role = st.selectbox("Role", placeholder=format_role_label(user.role), options=list(role_options.keys()))
        submit = st.form_submit_button("Edit")

        if submit:
            payload = {
                "fullname": fullname if fullname else user.fullname,
                "email": email if email else user.email,
                "role": role_options[role] if role else user.role,
            }
            try:
                with httpx.Client(timeout=120) as client:
                    response = client.patch(
                        f"{host}/api/accounts/{user.user_id}",
                        json=payload,
                        headers=_auth_headers(token),
                    )
                    response.raise_for_status()
                    response_data = response.json()
                if response_data.get("success"):
                    st.success("Account updated successfully!")
                    st.rerun()
                st.error(response_data.get("detail") or "Unable to update account.")
            except httpx.HTTPStatusError as error:
                detail = _format_error_detail(
                    error.response.json().get("detail", "Unable to update account.")
                )
                st.error(detail)
            except Exception as error:
                st.error(f"Error updating account: {error}")

    if st.button("Delete User", type="primary"):
        try:
            with httpx.Client(timeout=120) as client:
                response = client.delete(
                    f"{host}/api/delete_account/{user.user_id}",
                    headers=_auth_headers(token),
                )
                response.raise_for_status()
            st.success("Account deleted successfully!")
            st.rerun()
        except httpx.HTTPStatusError as error:
            detail = _format_error_detail(
                error.response.json().get("detail", "Unable to delete account.")
            )
            st.error(detail)
        except Exception as error:
            st.error(f"Error deleting account: {error}")
