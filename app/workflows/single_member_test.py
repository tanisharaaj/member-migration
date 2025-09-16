from temporalio import workflow
from typing import Dict, Any
from app.utils.invite_links import generate_invite_url

EMAIL_TIMEOUT = workflow.timedelta(minutes=3)

@workflow.defn(name="TestSingleMemberWorkflow")
class TestSingleMemberWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Hardcode test member
        test_email = "alex+2110@oakmorelabs.com"
        client_id = 1848

        # Generate unique invite link (deterministic time)
        invite_url = generate_invite_url(
            test_email,
            "cm7ai8xaa00006bd7bfhmskz3",
            now=workflow.now(),  # 👈 critical change
        )

        dynamic_data: Dict[str, Any] = {
            "client_id": client_id,
            "brand_name": "Test Brand",
            "app_name": "Test App",
            "invite_url": invite_url,
        }

        status_code = await workflow.execute_activity(
            "send_member_email_type3_activity",
            args=(test_email, dynamic_data),
            schedule_to_close_timeout=EMAIL_TIMEOUT,
        )

        return f"Sent test email to {test_email}, status={status_code}"
