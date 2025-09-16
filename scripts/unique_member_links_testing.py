@workflow.run
async def run(self, inp: BatchInput) -> BatchResult:
    # TEMPORARY: skip to Phase 3
    client_ids_from_sheet = [1939]   # or pull from sheet
    results: List[ItemResult] = []

    # Directly run Phase 3 members
    for client_id in client_ids_from_sheet:
        member_emails: List[str] = await workflow.execute_activity(
            "get_member_emails_activity",
            args=(client_id,),
            schedule_to_close_timeout=DB_TIMEOUT,
        )
        for to_email in member_emails:
            invite_url = generate_invite_url(to_email, "cm7ai8xaa00006bd7bfhmskz3")
            dynamic_data = {
                "client_id": client_id,
                "invite_url": invite_url,
                "brand_name": inp.brand_name,
            }
            await workflow.execute_activity(
                "send_member_email_type3_activity",
                args=(to_email, dynamic_data),
                schedule_to_close_timeout=EMAIL_TIMEOUT,
            )

