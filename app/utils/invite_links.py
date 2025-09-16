import jwt
from datetime import datetime, timedelta, timezone
from temporalio import workflow

SECRET_KEY = "GeorgeWashington123"  # TODO: move to env
ORIGIN = "https://www.attentivex.com"

def generate_invite_url(email: str, company_id: str, now: datetime = None, applications=None) -> str:
    if applications is None:
        applications = ["HEALTHCARE_PORTAL", "HEALTHCARE_MOBILE"]

    # Pick deterministic workflow time if inside a workflow
    if now is None:
        try:
            now = workflow.now()   # always UTC
        except RuntimeError:
            now = datetime.now(timezone.utc)  # fallback for local testing

    exp = now + timedelta(days=14)

    payload = {
        "email": email,
        "set_new_pw": True,
        "applications": applications,
        "company_id": company_id,
        "origin": ORIGIN,
        "iat": int(now.timestamp()),   # numeric
        "exp": int(exp.timestamp()),   # numeric
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return f"{ORIGIN}/confirm-invitation?token={token}"
