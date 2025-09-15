from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from collections import defaultdict
from temporalio import workflow


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


@dataclass
class ItemResult:
    client_id: int
    status: str
    detail: str


@dataclass
class BatchResult:
    tab_name: str
    processed: List[ItemResult]



DB_TIMEOUT = workflow.timedelta(minutes=2)
EMAIL_TIMEOUT = workflow.timedelta(minutes=3)

@workflow.defn(name="BrokerNotifyWorkflow")
class BrokerNotifyWorkflow:
    @workflow.run
    async def run(self, inp: BatchInput) -> BatchResult:
        # 1. read all rows
        rows = await workflow.execute_activity(
            "read_rows_activity",
            args=(inp.tab_name,),
            schedule_to_close_timeout=DB_TIMEOUT,
        )

        # preload valid client_ids from DB
        valid_client_ids: list[int] = await workflow.execute_activity(
            "get_all_client_ids_activity",
            schedule_to_close_timeout=DB_TIMEOUT,
        )

        results: List[ItemResult] = []
        broker_to_clients: Dict[int, List[int]] = defaultdict(list)
        client_ids_from_sheet: List[int] = []

        # 2. validate rows AND map brokers in one pass
        for row in rows:
            try:
                client_id = int(str(row.get("Client id")).strip())
            except Exception:
                results.append(
                    ItemResult(client_id=-1, status="skipped", detail="invalid client id")
                )
                continue

            if client_id not in valid_client_ids:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="skipped",
                        detail="client_id not found in DB"
                    )
                )
                continue

            client_ids_from_sheet.append(client_id)

            broker_ids: List[int] = await workflow.execute_activity(
                "get_broker_ids_for_client_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )

            if not broker_ids:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="not_found",
                        detail="no broker mapping in clients_to_brokers",
                    )
                )
                continue

            for bid in broker_ids:
                broker_to_clients[bid].append(client_id)


        # -----------------
        # Phase 1
        # -----------------

        # 3. send one email per unique broker_id (Broker Template 1)
        for broker_id, client_ids in broker_to_clients.items():
            to_email: Optional[str] = await workflow.execute_activity(
                "get_broker_email_activity",
                args=(broker_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not to_email:
                for cid in client_ids:
                    results.append(
                        ItemResult(
                            client_id=cid,
                            status="not_found",
                            detail=f"no email for broker {broker_id}",
                        )
                    )
                continue

            dynamic_data: Dict[str, Any] = {
                "client_ids": client_ids,
                "broker_id": broker_id,
                "brand_name": inp.brand_name,
                "app_name": inp.app_name,
                "appstore_link": inp.appstore_link,
                "playstore_link": inp.playstore_link,
                "website_portal": inp.website_portal,
                "cta_url": inp.cta_url,
            }

            status_code = await workflow.execute_activity(
                "send_broker_email_type1_activity",   # Phase 1 broker template
                args=(to_email, dynamic_data),
                schedule_to_close_timeout=EMAIL_TIMEOUT,
            )
            for cid in client_ids:
                results.append(
                    ItemResult(
                        client_id=cid,
                        status="sent",
                        detail=f"phase1_broker_email:{status_code}",
                    )
                )

        # 4. wait 2 minutes before client notifications
        await workflow.sleep(workflow.timedelta(minutes=2))

        # 5. send client emails (Client Template 1)
        for client_id in client_ids_from_sheet:
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not emails:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="not_found",
                        detail="no client contact emails found",
                    )
                )
                continue

            for to_email in emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                }
                status_code = await workflow.execute_activity(
                    "send_client_email_type1_activity",   # Phase 1 client template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase1_client_email:{to_email}:{status_code}",
                    )
                )

        # 6. wait 2 minutes before member notifications
        await workflow.sleep(workflow.timedelta(minutes=2))

        # 7. send member emails (Member Template 1)
        for client_id in client_ids_from_sheet:
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="skipped",
                        detail="no ACTIVE members found for client_id",
                    )
                )
                continue

            for to_email in member_emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                }
                status_code = await workflow.execute_activity(
                    "send_member_email_type1_activity",   # Phase 1 member template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase1_member_email:{to_email}:{status_code}",
                    )
                )

        # -----------------
        # Phase 2 (reminders)
        # -----------------

        # 8. wait 5 minutes before Phase 2 starts
        await workflow.sleep(workflow.timedelta(minutes=5))

        # 9. Phase 2 broker emails (Broker Template 2)
        for broker_id, client_ids in broker_to_clients.items():
            to_email: Optional[str] = await workflow.execute_activity(
                "get_broker_email_activity",
                args=(broker_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not to_email:
                continue

            dynamic_data: Dict[str, Any] = {
                "client_ids": client_ids,
                "broker_id": broker_id,
                "brand_name": inp.brand_name,
                "app_name": inp.app_name,
                "appstore_link": inp.appstore_link,
                "playstore_link": inp.playstore_link,
                "website_portal": inp.website_portal,
                "cta_url": inp.cta_url,
                "launch_date": inp.launch_date,
            }
            status_code = await workflow.execute_activity(
                "send_broker_email_type2_activity",   # Phase 2 broker template
                args=(to_email, dynamic_data),
                schedule_to_close_timeout=EMAIL_TIMEOUT,
            )
            for cid in client_ids:
                results.append(
                    ItemResult(
                        client_id=cid,
                        status="sent",
                        detail=f"phase2_broker_email:{status_code}",
                    )
                )

        # 10. wait 2 minutes before Phase 2 clients
        await workflow.sleep(workflow.timedelta(minutes=2))

        # 11. Phase 2 client emails (Client Template 2)
        for client_id in client_ids_from_sheet:
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            for to_email in emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                }
                status_code = await workflow.execute_activity(
                    "send_client_email_type2_activity",   # Phase 2 client template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase2_client_email:{to_email}:{status_code}",
                    )
                )

        # 12. wait 2 minutes before Phase 2 members
        await workflow.sleep(workflow.timedelta(minutes=2))

        # 13. Phase 2 member emails (Member Template 2)
        for client_id in client_ids_from_sheet:
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="skipped",
                        detail="no ACTIVE members found for client_id",
                    )
                )
                continue

            for to_email in member_emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                }
                status_code = await workflow.execute_activity(
                    "send_member_email_type2_activity",   # Phase 2 member template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase2_member_email:{to_email}:{status_code}",
                    )
                )

        # -----------------
        # Phase 3 (final follow-ups)
        # -----------------

        # 13b. wait 5 minutes before Phase 3 starts
        await workflow.sleep(workflow.timedelta(minutes=5))

        # 13c. Phase 3 client emails (Client Template 3)
        for client_id in client_ids_from_sheet:
            emails: List[str] = await workflow.execute_activity(
                "get_client_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            for to_email in emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                    "launch_date": inp.launch_date,
                }
                status_code = await workflow.execute_activity(
                    "send_client_email_type3_activity",   # Phase 3 client template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase3_client_email:{to_email}:{status_code}",
                    )
                )

        # 13d. wait 2 minutes before Phase 3 members
        await workflow.sleep(workflow.timedelta(minutes=2))

        # 13e. Phase 3 member emails (Member Template 3)
        for client_id in client_ids_from_sheet:
            member_emails: List[str] = await workflow.execute_activity(
                "get_member_emails_activity",
                args=(client_id,),
                schedule_to_close_timeout=DB_TIMEOUT,
            )
            if not member_emails:
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="skipped",
                        detail="no ACTIVE members found for client_id",
                    )
                )
                continue

            for to_email in member_emails:
                dynamic_data: Dict[str, Any] = {
                    "client_id": client_id,
                    "brand_name": inp.brand_name,
                    "app_name": inp.app_name,
                    "appstore_link": inp.appstore_link,
                    "playstore_link": inp.playstore_link,
                    "website_portal": inp.website_portal,
                    "cta_url": inp.cta_url,
                    "launch_date": inp.launch_date,
                }
                status_code = await workflow.execute_activity(
                    "send_member_email_type3_activity",   # Phase 3 member template
                    args=(to_email, dynamic_data),
                    schedule_to_close_timeout=EMAIL_TIMEOUT,
                )
                results.append(
                    ItemResult(
                        client_id=client_id,
                        status="sent",
                        detail=f"phase3_member_email:{to_email}:{status_code}",
                    )
                )

        # 14. return batch result
        return BatchResult(tab_name=inp.tab_name, processed=results)
