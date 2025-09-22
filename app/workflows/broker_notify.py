from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from collections import defaultdict
from temporalio import workflow
from app.utils.invite_links import generate_invite_url

# Input data structure for batch processing
@dataclass
class BatchInput:
    tab_name: str
    brand_name: Optional[str] = None
    app_name: Optional[str] = None
    appstore_link: Optional[str] = None
    playstore_link: Optional[str] = None
    website_portal: Optional[str] = None
    cta_url: Optional[str] = None
    launch_date: Optional[str] = None

# Result for each processed item (client)
@dataclass
class ItemResult:
    client_id: int
    status: str
    detail: str

# Result for the entire batch
@dataclass
class BatchResult:
    tab_name: str
    processed: List[ItemResult]

# Timeout settings for activities
DB_TIMEOUT = workflow.timedelta(minutes=2)
EMAIL_TIMEOUT = workflow.timedelta(minutes=3)

# Helper to build dynamic data for email templates
def build_dynamic_data(client_id: int, inp: BatchInput, extra: Dict[str, Any] = None) -> Dict[str, Any]:
    data = {
        "client_id": client_id,
        "brand_name": inp.brand_name,
        "app_name": inp.app_name,
        "appstore_link": inp.appstore_link,
        "playstore_link": inp.playstore_link,
        "website_portal": inp.website_portal,
        "cta_url": inp.cta_url,
        "launch_date": inp.launch_date,
    }
    if extra:
        data.update(extra)
    return data

# Main workflow definition for broker notification
@workflow.defn(name="BrokerNotifyWorkflow")
class BrokerNotifyWorkflow:
    @workflow.run
    async def run(self, inp: BatchInput) -> BatchResult:
        # Read rows from input tab
        rows = await workflow.execute_activity(
            "read_rows_activity",
            args=(inp.tab_name,),
            schedule_to_close_timeout=DB_TIMEOUT,
        )
        # Get all valid client IDs from DB
        valid_client_ids: list[int] = await workflow.execute_activity(
            "get_all_client_ids_activity",
            schedule_to_close_timeout=DB_TIMEOUT,
        )

        results: List[ItemResult] = []
        broker_to_clients: Dict[int, List[int]] = defaultdict(list)
        client_ids_from_sheet: List[int] = []

        # Validate rows and map brokers to clients
        for row in rows:
            try:
                client_id = int(str(row.get("Client id")).strip())
            except Exception:
                results.append(ItemResult(-1, "skipped", "invalid client id"))
                continue
            if client_id not in valid_client_ids:
                results.append(ItemResult(client_id, "skipped", "client_id not found in DB"))
                continue
            client_ids_from_sheet.append(client_id)
            # Get broker IDs for each client
            broker_ids: List[int] = await workflow.execute_activity(
                "get_broker_ids_for_client_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not broker_ids:
                results.append(ItemResult(client_id, "not_found", "no broker mapping in clients_to_brokers"))
                continue
            for bid in broker_ids:
                broker_to_clients[bid].append(client_id)

        # Phase 1: Send emails to brokers, clients, and members
        await self.send_broker_emails_phase1(broker_to_clients, inp, results)
        await workflow.sleep(workflow.timedelta(minutes=2))
        await self.send_client_emails_phase1(client_ids_from_sheet, inp, results)
        await workflow.sleep(workflow.timedelta(minutes=2))
        await self.send_member_emails_phase1(client_ids_from_sheet, inp, results)

        # Phase 2: Send reminder emails
        await workflow.sleep(workflow.timedelta(minutes=5))
        await self.send_broker_emails_phase2(broker_to_clients, inp, results)
        await workflow.sleep(workflow.timedelta(minutes=2))
        await self.send_client_emails_phase2(client_ids_from_sheet, inp, results)
        await workflow.sleep(workflow.timedelta(minutes=2))
        await self.send_member_emails_phase2(client_ids_from_sheet, inp, results)

        # Phase 3: Final follow-up emails
        await workflow.sleep(workflow.timedelta(minutes=5))
        await self.send_client_emails_phase3(client_ids_from_sheet, inp, results)
        await workflow.sleep(workflow.timedelta(minutes=2))
        await self.send_member_emails_phase3(client_ids_from_sheet, inp, results)

        return BatchResult(tab_name=inp.tab_name, processed=results)

    # Phase 1: Send emails to brokers
    async def send_broker_emails_phase1(self, broker_to_clients, inp, results):
        for broker_id, client_ids in broker_to_clients.items():
            # Get broker email
            to_email: Optional[str] = await workflow.execute_activity(
                "get_broker_email_activity",
                args=(broker_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not to_email:
                for cid in client_ids:
                    results.append(ItemResult(cid, "not_found", f"no email for broker {broker_id}"))
                continue
            for cid in client_ids:
                # Get client name for email personalization
                client_name: Optional[str] = await workflow.execute_activity(
                    "get_client_name_activity",
                    args=(cid,),
                    schedule_to_close_timeout=DB_TIMEOUT,
                )
                dynamic_data = build_dynamic_data(cid, inp, {"broker_id": broker_id, "client_name": client_name})
                # Send broker email (phase 1)
                status_code = await workflow.execute_activity(
                    "send_broker_email_type1_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(cid, "sent", f"phase1_broker_email:{status_code}"))

    # Phase 1: Send emails to clients
    async def send_client_emails_phase1(self, client_ids, inp, results):
        for client_id in client_ids:
            # Get client contact emails
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not emails:
                results.append(ItemResult(client_id, "not_found", "no client contact emails found"))
                continue
            for to_email in emails:
                dynamic_data = build_dynamic_data(client_id, inp)
                # Send client email (phase 1)
                status_code = await workflow.execute_activity(
                    "send_client_email_type1_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(client_id, "sent", f"phase1_client_email:{to_email}:{status_code}"))

    # Phase 1: Send emails to members
    async def send_member_emails_phase1(self, client_ids, inp, results):
        for client_id in client_ids:
            # Get member emails for client
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(ItemResult(client_id, "skipped", "no ACTIVE members found for client_id"))
                continue
            for to_email in member_emails:
                dynamic_data = build_dynamic_data(client_id, inp)
                # Send member email (phase 1)
                status_code = await workflow.execute_activity(
                    "send_member_email_type1_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(client_id, "sent", f"phase1_member_email:{to_email}:{status_code}"))

    # Phase 2: Send reminder emails to brokers
    async def send_broker_emails_phase2(self, broker_to_clients, inp, results):
        for broker_id, client_ids in broker_to_clients.items():
            # Get broker email
            to_email: Optional[str] = await workflow.execute_activity(
                "get_broker_email_activity",
                args=(broker_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not to_email:
                for cid in client_ids:
                    results.append(ItemResult(cid, "not_found", f"no email for broker {broker_id}"))
                continue
            for cid in client_ids:
                # Get client name for email personalization
                client_name: Optional[str] = await workflow.execute_activity(
                    "get_client_name_activity",
                    args=(cid,),
                    schedule_to_close_timeout=DB_TIMEOUT,
                )
                dynamic_data = build_dynamic_data(cid, inp, {"broker_id": broker_id, "client_name": client_name})
                # Send broker email (phase 1)
                status_code = await workflow.execute_activity(
                    "send_broker_email_type2_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(cid, "sent", f"phase2_broker_email:{status_code}"))

    # Phase 2: Send reminder emails to clients
    async def send_client_emails_phase2(self, client_ids, inp, results):
        for client_id in client_ids:
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            for to_email in emails:
                dynamic_data = build_dynamic_data(client_id, inp)
                # Send client reminder email (phase 2)
                status_code = await workflow.execute_activity(
                    "send_client_email_type2_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(client_id, "sent", f"phase2_client_email:{to_email}:{status_code}"))

    # Phase 2: Send reminder emails to members
    async def send_member_emails_phase2(self, client_ids, inp, results):
        for client_id in client_ids:
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(ItemResult(client_id, "skipped", "no ACTIVE members found for client_id"))
                continue
            for to_email in member_emails:
                dynamic_data = build_dynamic_data(client_id, inp)
                # Send member reminder email (phase 2)
                status_code = await workflow.execute_activity(
                    "send_member_email_type2_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(client_id, "sent", f"phase2_member_email:{to_email}:{status_code}"))

    # Phase 3: Send final follow-up emails to clients
    async def send_client_emails_phase3(self, client_ids, inp, results):
        for client_id in client_ids:
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            for to_email in emails:
                dynamic_data = build_dynamic_data(client_id, inp)
                # Send client final follow-up email (phase 3)
                status_code = await workflow.execute_activity(
                    "send_client_email_type3_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(client_id, "sent", f"phase3_client_email:{to_email}:{status_code}"))

    # Phase 3: Send final follow-up emails to members
    async def send_member_emails_phase3(self, client_ids, inp, results):
        for client_id in client_ids:
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(ItemResult(client_id, "skipped", "no ACTIVE members found for client_id"))
                continue
            for to_email in member_emails:
                # Insert member account before sending invite
                insert_result = await workflow.execute_activity(
                    "insert_member_accounts_activity",
                    args=(to_email, "cm7ai8xaa00006bd7bfhmskz3"),
                    schedule_to_close_timeout=DB_TIMEOUT,
                )
                # Generate invite URL for member
                invite_url = generate_invite_url(
                    email=to_email,
                    company_id="cm7ai8xaa00006bd7bfhmskz3",
                )
                dynamic_data = build_dynamic_data(client_id, inp, {"invite_url": invite_url})
                # Send member final follow-up email (phase 3)
                status_code = await workflow.execute_activity(
                    "send_member_email_type3_activity",
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(ItemResult(
                    client_id,
                    "sent",
                    f"phase3_member_email:{to_email}:{status_code} "
                    f"(portal_id={insert_result['portal_id']}, mobile_id={insert_result['mobile_id']})"
                ))
