"""
scripts/test_broker_client_member_notifications.py

This script tests the full BrokerNotifyWorkflow:
1. Reads client IDs from a Google Sheet tab.
2. Sends broker notification emails (Broker Template 1).
3. Waits 2 minutes.
4. Sends client notification emails (Client Template 1).
5. Waits 2 minutes.
6. Sends member notification emails (Member Template 1).
7. Returns a BatchResult with ItemResults for each stage.

Usage:
    python scripts/test_broker_client_member_notifications.py
Make sure:
- The worker is running and registered with Temporal.
- DATA_API, Google Sheets, and SendGrid configs are set in .env.
"""

import asyncio
import uuid
from temporalio.client import Client
from app.settings import settings
from app.workflows.broker_notify import BrokerNotifyWorkflow, BatchInput


async def main():
    # 1. connect to Temporal Cloud
    client = await Client.connect(
        target_host=settings.TEMPORAL_HOST,
        namespace=settings.TEMPORAL_NAMESPACE,
        api_key=settings.TEMPORAL_API_KEY,
        tls=True,
    )

    # 2. unique workflow id for this run
    workflow_id = f"broker-client-member-test-{uuid.uuid4().hex[:6]}"

    # 3. start workflow
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
        ),
        id=workflow_id,
        task_queue="broker-notify-queue",
    )

    # 4. wait for workflow to finish
    result = await handle.result()

    # 5. pretty print results
    print("\n Workflow finished")
    print("Workflow ID:", workflow_id)
    print("Batch tab:", result.tab_name)
    print("\nProcessed results:")
    for item in result.processed:
        print(f"  client_id={item.client_id}, status={item.status}, detail={item.detail}")


if __name__ == "__main__":
    asyncio.run(main())

