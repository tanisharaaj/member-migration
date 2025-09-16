# activities/accounts.py
import uuid
import datetime
import requests
from app.settings import settings


def insert_member_accounts(email: str, company_id: str) -> dict:
    # generate cuid-like IDs
    portal_account_id = "cm" + uuid.uuid4().hex[:22]
    mobile_account_id = "cm" + uuid.uuid4().hex[:22]
    now = datetime.datetime.utcnow().isoformat()

    accounts = [
        {
            "operation": "insert",
            "table": "accounts",
            "fields": {
                "id": None,
                "email": email,
                "status": "INVITED",
                "user_id": portal_account_id,
                "company_id": company_id,
                "created_at": now,
                "updated_at": now,
                "application": "HEALTHCARE_PORTAL"
            }
        },
        {
            "operation": "insert",
            "table": "accounts",
            "fields": {
                "id": None,
                "email": email,
                "status": "INVITED",
                "user_id": mobile_account_id,
                "company_id": company_id,
                "created_at": now,
                "updated_at": now,
                "application": "HEALTHCARE_MOBILE"
            }
        },
    ]

    results = []
    for acc in accounts:
        resp = requests.post(
            f"{settings.DATA_API_BASE_URL}/crud",
            params={"db": settings.DATA_API_ACCOUNTS_DB_KEY},  # 👈 use the new DB key
            headers={"Authorization": f"Bearer {settings.AUTH_STATIC_BEARER_TOKEN}"},
            json=acc,
            timeout=120,
        )
        resp.raise_for_status()
        results.append(resp.json())

    return {"portal_id": portal_account_id, "mobile_id": mobile_account_id}
