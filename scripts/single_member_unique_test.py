import asyncio
from temporalio.client import Client
from app.settings import settings
from app.workflows.single_member_test import TestSingleMemberWorkflow
import uuid

async def main():
    client = await Client.connect(
        target_host=settings.TEMPORAL_HOST,
        namespace=settings.TEMPORAL_NAMESPACE,
        api_key=settings.TEMPORAL_API_KEY,
        tls=True,
    )

    handle = await client.start_workflow(
        TestSingleMemberWorkflow.run,
        id=f"single-member-test-{uuid.uuid4().hex}",  # unique each run
        task_queue="broker-notify-queue",
    )

    print("Workflow started:", handle.id)
    result = await handle.result()
    print("Workflow result:", result)

if __name__ == "__main__":
    asyncio.run(main())
