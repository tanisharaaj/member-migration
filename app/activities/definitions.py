from temporalio import activity
from app.activities import sheets, data_api, email
from app.activities.accounts.accounts import insert_member_accounts



# --- Sheets ---
@activity.defn
async def read_rows_activity(tab_name: str):
    """Read rows from a Google Sheet tab."""
    return sheets.read_batch_rows(tab_name)

# --- Members---
@activity.defn
async def get_member_emails_activity(client_id: int) -> list[str]:
    return data_api.get_member_emails_by_client_id(client_id)



from temporalio import activity
from app.activities.accounts.accounts import insert_member_accounts

@activity.defn
def insert_member_accounts_activity(email: str, company_id: str) -> dict:
    return insert_member_accounts(email, company_id)




# --- Brokers ---
@activity.defn
async def get_broker_ids_for_client_activity(client_id: int):
    """Return list of broker_ids for a given client_id from clients_to_brokers."""
    return data_api.get_broker_ids_for_client(client_id)


@activity.defn
async def get_broker_email_activity(broker_id: int):
    """Return broker email for a given broker_id from brokers table."""
    return data_api.get_broker_email_by_id(broker_id)


# --- Clients ---
@activity.defn
async def get_client_emails_activity(client_id: int) -> list[str]:
    """Return list of client contact emails from client_contacts table."""
    return data_api.get_client_emails_by_id(client_id)


@activity.defn
async def get_all_client_ids_activity() -> list[int]:
    return data_api.get_all_client_ids()


@activity.defn
async def get_client_name_activity(client_id: int) -> str:
    """Return the client_name from the clients table."""
    return data_api.get_client_name_by_id(client_id)


# --- Emails: Brokers ---
@activity.defn
async def send_broker_email_type1_activity(to_email: str, dynamic_data: dict):
    """Send broker email using template 1."""
    return email.send_broker_email_type1(to_email, dynamic_data)


@activity.defn
async def send_broker_email_type2_activity(to_email: str, dynamic_data: dict):
    """Send broker email using template 2."""
    return email.send_broker_email_type2(to_email, dynamic_data)


# --- Emails: Clients ---
@activity.defn
async def send_client_email_type1_activity(to_email: str, dynamic_data: dict):
    """Send client email using template 1."""
    return email.send_client_email_type1(to_email, dynamic_data)


@activity.defn
async def send_client_email_type2_activity(to_email: str, dynamic_data: dict):
    """Send client email using template 2."""
    return email.send_client_email_type2(to_email, dynamic_data)
    
    
@activity.defn
async def send_client_email_type3_activity(to_email: str, dynamic_data: dict):
    """Send client email using template 3."""
    return email.send_client_email_type3(to_email, dynamic_data) 


# --- Emails: Members ---

@activity.defn
async def insert_member_accounts_activity(email: str, company_id: str):
    """Insert portal + mobile accounts for a member in the accounts table."""
    return insert_member_accounts(email, company_id)


@activity.defn
async def send_member_email_type1_activity(to_email: str, dynamic_data: dict):
    """Send member email using template 1."""
    return email.send_member_email_type1(to_email, dynamic_data)


@activity.defn
async def send_member_email_type2_activity(to_email: str, dynamic_data: dict):
    """Send member email using template 2."""
    return email.send_member_email_type2(to_email, dynamic_data)

@activity.defn
async def send_member_email_type3_activity(to_email: str, dynamic_data: dict):
    """Send member email using template 3."""
    return email.send_member_email_type3(to_email, dynamic_data)