# Secret Rotation Checklist

Use this checklist whenever a real secret is committed, shared in chat, copied
into an unsafe file, exposed in logs, or handed to an untrusted environment.

## Immediate Actions

1. Revoke or rotate the exposed secret in the original provider.
2. Replace the value in the deployment environment.
3. Restart affected services after the new secret is active.
4. Confirm the app can authenticate to the provider with the new value.
5. Remove the secret from local working copies where possible.

## Project Secrets to Review

- `CSRF_SECRET`
- `JWT_SECRET_KEY`
- `JWT_REFRESH_SECRET_KEY`
- `APP_ENCRYPTION_KEY`
- `INITIAL_SUPERADMIN_PASSWORD`
- Google service-account credentials
- Google Ads OAuth client secret and developer token
- Meta app secret and page/access tokens
- YouTube OAuth client secret and refresh tokens

## Repository Hygiene

- Keep real values in `.env`, deployment secrets, or `*_FILE` mounted secret files.
- Keep `.env.example` limited to placeholders.
- Do not paste provider JSON credentials into README, PRD, tests, or issue text.
- After a leak, assume git history and screenshots may still contain the old value.

## Verification

- `python init_db.py` or `python migrate_db.py` runs successfully.
- Backend `/health` returns `status=ok`.
- Streamlit login still works.
- Token-management pages can read provider status without exposing token values.
