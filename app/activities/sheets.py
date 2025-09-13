# app/activities/sheets.py
import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict
from app.settings import settings

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def _client():
    info = settings.service_account_info()
    if not info:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def list_tabs() -> List[str]:
    gc = _client()
    sh = gc.open_by_key(settings.SHEET_ID)
    return [ws.title for ws in sh.worksheets()]

def read_batch_rows(tab_name: str) -> List[Dict]:
    """
    Returns all rows from the tab as a list of dicts.
    Each dict looks like {"Client Name": "...", "Client id": "..."}.
    """
    gc = _client()
    sh = gc.open_by_key(settings.SHEET_ID)
    ws = sh.worksheet(tab_name)
    rows = ws.get_all_records(head=1)
    return rows
