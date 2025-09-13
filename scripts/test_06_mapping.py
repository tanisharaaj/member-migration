"""
scripts/test_06_mapping.py

This script tests the new broker lookup logic end-to-end (without Temporal).
It directly calls the data_api helpers to verify that:

1. For a given client_id, we can fetch the broker_ids from `clients_to_brokers`.
2. For each broker_id, we can fetch the broker's email from `brokers`.
3. The final mapping matches the expected shape: {broker_id: email}.
"""

from app.activities import data_api


def test_mapping(client_id: int):
    print(f"Testing mapping for client_id={client_id}")

    # Step 1: get broker_ids for this client
    broker_ids = data_api.get_broker_ids_for_client(client_id)
    print("Broker IDs from clients_to_brokers:", broker_ids)

    if not broker_ids:
        print("No broker mapping found for client_id", client_id)
        return

    # Step 2: for each broker_id, fetch email
    broker_map = {}
    for bid in broker_ids:
        email = data_api.get_broker_email_by_id(bid)
        broker_map[bid] = email

    # Step 3: show results
    for bid, email in broker_map.items():
        if email:
            print(f" Broker {bid} -> {email}")
        else:
            print(f"Broker {bid} has no email in brokers table")


if __name__ == "__main__":
    # change this client_id to test others
    test_mapping(1)
    test_mapping(2)
