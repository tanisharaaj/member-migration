# app/activities/data_api.py
import requests
from typing import Optional, Dict, Any
from app.settings import settings


import re

def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {settings.AUTH_STATIC_BEARER_TOKEN}"}


def _select(table: str, columns: list[str], filters: Dict[str, Any]) -> list[Dict[str, Any]]:
    url = f"{settings.DATA_API_BASE_URL}/select"
    params = {"db": settings.DATA_API_DB_KEY}
    body = {"table": table, "columns": columns, "filters": filters}
    headers = _headers()

    resp = requests.post(url, json=body, params=params, headers=headers, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    # handle "result" vs "rows"
    if "rows" in data:
        return data["rows"]
    elif "result" in data:
        return data["result"]
    else:
        return []


def get_broker_ids_for_client(client_id: int) -> list[int]:
    rows = _select(
        table="clients_to_brokers",
        columns=["broker_id"],
        filters={"client_id": client_id},
    )
    return [int(r["broker_id"]) for r in rows] if rows else []


def get_broker_email_by_id(broker_id: int) -> Optional[str]:
    rows = _select(
        table="brokers",
        columns=["email"],
        filters={"id": broker_id},
    )
    return rows[0]["email"] if rows else None
    
    


def get_client_emails_by_id(client_id: int) -> list[str]:
    rows = _select(
        table="client_contacts",
        columns=["email"],
        filters={"client_id": client_id},
    )
    # return all non-empty emails
    return [r["email"] for r in rows if r.get("email")] if rows else []





def get_member_emails_by_client_id(client_id: int) -> list[str]:
    # 1. Get all members for this client
    members = _select(
        table="members",
        columns=["id", "email"],
        filters={"client_id": client_id},
    )
    if not members:
        print(f"[DEBUG] No members found for client_id={client_id}")
        return []

    active_emails: list[str] = []

    # 2. For each member, check if ACTIVE in current_member_status_view
    for m in members:
        member_id = m.get("id")
        email = m.get("email")
        if not member_id or not email:
            print(f"[DEBUG] Skipping member with missing id/email: {m}")
            continue

        status_rows = _select(
            table="current_member_status_view",
            columns=["member_status"],
            filters={"member_id": member_id},
        )

        if status_rows:
            statuses = [r.get("member_status") for r in status_rows]
            print(f"[DEBUG] Member {member_id}: statuses={statuses}")
        else:
            print(f"[DEBUG] Member {member_id}: no status rows found")

        # 3. Only keep ACTIVE members
        if status_rows and any(r.get("member_status") == "ACTIVE" for r in status_rows):
            active_emails.append(email)
            print(f"[DEBUG] Member {member_id} is ACTIVE, added email={email}")
        else:
            print(f"[DEBUG] Member {member_id} not ACTIVE, skipped")

    return active_emails


def get_all_client_ids() -> list[int]:
    rows = _select(
        table="clients",
        columns=["id"],
        filters={}  # no filter → return all clients
    )
    return [int(r["id"]) for r in rows] if rows else []


def get_client_name_by_id(client_id: int) -> Optional[str]:
    rows = _select(
        table="clients",
        columns=["client_name"],
        filters={"id": client_id},
    )
    return rows[0]["client_name"] if rows else None



