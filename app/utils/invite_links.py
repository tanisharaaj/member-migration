import jwt
import os
from datetime import datetime, timedelta, timezone
from temporalio import workflow

# Read directly from environment (safe inside workflows)
SECRET_KEY = os.getenv("INVITE_SECRET_KEY", "default-secret")
ORIGIN = os.getenv("INVITE_ORIGIN", "https://example.com")


def generate_invite_url(email: str, company_id: str, now: datetime = None, applications=None) -> str:
    if applications is None:
        applications = ["HEALTHCARE_PORTAL", "HEALTHCARE_MOBILE"]

    # Deterministic workflow time if inside Temporal
    if now is None:
        try:
            now = workflow.now()   # always UTC inside workflow
        except RuntimeError:
            now = datetime.now(timezone.utc)  # fallback for local/local testing

    exp = now + timedelta(days=14)

    payload = {
        "email": email,
        "set_new_pw": True,
        "applications": applications,
        "company_id": company_id,
        "origin": ORIGIN,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return f"{ORIGIN}/confirm-invitation?token={token}"
