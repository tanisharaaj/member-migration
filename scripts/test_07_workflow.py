"""
scripts/test_07_workflow.py

This script tests the full BrokerNotifyWorkflow against Temporal Cloud.

What it verifies:
1. We can start the workflow with a BatchInput (pointing to a Google Sheet tab).
2. The workflow correctly:
   - reads rows from the sheet
   - maps client_id -> broker_id(s) via clients_to_brokers
   - fetches broker emails via brokers table
   - deduplicates by broker
   - sends one email per broker (SendGrid activity)
3. The workflow returns a BatchResult with detailed ItemResult statuses.

Usage:
- Make sure your worker is running and has registered activities/workflows.
- Update the `tab_name` to match a real Google Sheet tab with client_ids.
- Run: `python scripts/test_07_workflow.py`
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

    # 2. generate unique workflow id
    workflow_id = f"broker-notify-{uuid.uuid4().hex[:6]}"

    # 3. start workflow with BatchInput
    handle = await client.start_workflow(
        BrokerNotifyWorkflow.run,
        BatchInput(
            tab_name="Batch 1",  # 👈 update to your test sheet tab
            brand_name="Example Brand",
            app_name="Example App",
            appstore_link="https://apps.apple.com/app/id000000",
            playstore_link="https://play.google.com/store/apps/details?id=example",
            website_portal="https://example.com/portal",
            cta_url="https://example.com/action",
        ),
        id=workflow_id,
        task_queue="broker-notify-queue",
    )

    # 4. wait for workflow to finish and get result
    result = await handle.result()

    # 5. print outputs for inspection
    print("Workflow finished")
    print("Workflow ID:", workflow_id)
    print("Batch tab:", result.tab_name)
    print("Processed results:")
    for item in result.processed:
        print(f"  client_id={item.client_id}, status={item.status}, detail={item.detail}")


if __name__ == "__main__":
    asyncio.run(main())
