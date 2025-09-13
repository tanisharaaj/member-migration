# scripts/test_02_data_api_client_lookup.py
from app.activities.data_api import get_client_by_id


print(get_client_by_id(1))

print(get_client_by_id(2))
