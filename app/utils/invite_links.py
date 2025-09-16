import jwt
from datetime import datetime, timedelta, timezone
from temporalio import workflow
from app.settings import settings

def generate_invite_url(email: str, company_id: str, now: datetime = None, applications=None) -> str:
    if applications is None:
        applications = ["HEALTHCARE_PORTAL", "HEALTHCARE_MOBILE"]

    # Pick deterministic workflow time if inside a workflow
    if now is None:
        try:
            now = workflow.now()   # always UTC inside workflow
        except RuntimeError:
            now = datetime.now(timezone.utc)  # fallback for local testing

    exp = now + timedelta(days=14)

    payload = {
        "email": email,
        "set_new_pw": True,
        "applications": applications,
        "company_id": company_id,
        "origin": settings.INVITE_ORIGIN,
        "iat": int(now.timestamp()),   # numeric
        "exp": int(exp.timestamp()),   # numeric
    }

    token = jwt.encode(payload, settings.INVITE_SECRET_KEY, algorithm="HS256")
    return f"{settings.INVITE_ORIGIN}/confirm-invitation?token={token}"

