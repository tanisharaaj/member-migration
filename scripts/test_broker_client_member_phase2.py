"""
scripts/test_broker_client_member_phase2.py

Test the full BrokerNotifyWorkflow including Phase 1 and Phase 2:
    Phase 1:
        - Brokers (template 1)
        - Clients (template 1)
        - Members (template 1)
    Phase 2:
        - Brokers (template 2)
        - Clients (template 2)
        - Members (template 2)

Delays:
    - 2 min between brokers → clients
    - 2 min between clients → members
    - 5 min between Phase 1 → Phase 2

Usage:
    python scripts/test_broker_client_member_phase2.py
"""

import asyncio
import uuid
from temporalio.client import Client
from app.settings import settings
from app.workflows.broker_notify import BrokerNotifyWorkflow, BatchInput


async def main():
    # 1. Connect to Temporal Cloud
    client = await Client.connect(
        target_host=settings.TEMPORAL_HOST,
        namespace=settings.TEMPORAL_NAMESPACE,
        api_key=settings.TEMPORAL_API_KEY,
        tls=True,
    )

    # 2. Generate unique workflow ID
    workflow_id = f"broker-client-member-phase2-{uuid.uuid4().hex[:6]}"

    # 3. Start workflow
    handle = await client.start_workflow(
        BrokerNotifyWorkflow.run,
        BatchInput(
            tab_name="Batch 1",  # update with your test tab name
            brand_name="Test Brand",
            app_name="Test App",
            appstore_link="https://apps.apple.com/app/id000000",
            playstore_link="https://play.google.com/store/apps/details?id=test",
            website_portal="https://test.com/portal",
            cta_url="https://test.com/action",
            launch_date="2025-09-30", 
        ),
        id=workflow_id,
        task_queue="broker-notify-queue",
    )

    print(f"Workflow started: {workflow_id}")
    print("Waiting for Phase 1 + Phase 2 + Phase 3 to complete... (this will take ~18–20 minutes)")


    # 4. Wait for workflow result
    result = await handle.result()

    # 5. Print results
    print("\n Workflow finished successfully")
    print("Workflow ID:", workflow_id)
    print("Batch tab:", result.tab_name)

    print("\nProcessed results:")
    for item in result.processed:
        print(f"  client_id={item.client_id}, status={item.status}, detail={item.detail}")


if __name__ == "__main__":
    asyncio.run(main())

