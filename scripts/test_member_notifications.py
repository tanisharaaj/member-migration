import asyncio
from temporalio.client import Client
from uuid import uuid4

from app.settings import settings



from app.workflows.broker_notify import BrokerNotifyWorkflow, BatchInput


print("DEBUG: TEMPORAL_API_KEY =", repr(settings.TEMPORAL_API_KEY))


async def main():
    client = await Client.connect(
        target_host=settings.TEMPORAL_HOST,
        namespace=settings.TEMPORAL_NAMESPACE,
        api_key=settings.TEMPORAL_API_KEY,   # not a JWT, it's the API key
        tls=True,
    )




    # 🔹 Only test member notifications (skip brokers + clients for now)
    workflow_id = f"test-member-only-{uuid4().hex}"
    print(f"Starting workflow {workflow_id}...")

    handle = await client.start_workflow(
        BrokerNotifyWorkflow.run,
        BatchInput(
            tab_name="Batch 1",   # make a temporary test tab in Google Sheets
            brand_name="Test Brand",
            app_name="Test App",
            appstore_link="https://appstore.test/app",
            playstore_link="https://playstore.test/app",
            website_portal="https://test.com",
            cta_url="https://cta.test",
            launch_date="2025-09-15"
        ),
        id=workflow_id,
        task_queue="broker-notify-queue",
    )

    print("Workflow started. Waiting for result...")
    result = await handle.result()
    print("Workflow finished.")
    print("Result:", result)


if __name__ == "__main__":
    asyncio.run(main())

