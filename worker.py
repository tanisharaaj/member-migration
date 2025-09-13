import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from app.settings import settings
from app.workflows.broker_notify import BrokerNotifyWorkflow

print("DEBUG: SENDGRID_API_KEY prefix:", settings.SENDGRID_API_KEY[:8])
print("DEBUG: FROM_EMAIL:", settings.SENDGRID_FROM_EMAIL)


from app.activities.definitions import (
    # sheets + lookups
    read_rows_activity,
    get_broker_ids_for_client_activity,
    get_broker_email_activity,
    get_client_emails_activity,
    get_member_emails_activity,   
    get_all_client_ids_activity,


    # broker emails
    send_broker_email_type1_activity,
    send_broker_email_type2_activity,

    # client emails
    send_client_email_type1_activity,
    send_client_email_type2_activity,
    send_client_email_type3_activity,

    # member emails
    send_member_email_type1_activity,
    send_member_email_type2_activity,
    send_member_email_type3_activity,
    
)



async def main():
    client = await Client.connect(
        target_host=settings.TEMPORAL_HOST,
        namespace=settings.TEMPORAL_NAMESPACE,
        api_key=settings.TEMPORAL_API_KEY,
        tls=True,
    )

    task_queue = "broker-notify-queue"

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[BrokerNotifyWorkflow],
        activities=[
            # sheets + lookups
            read_rows_activity,
            get_broker_ids_for_client_activity,
            get_broker_email_activity,
            get_client_emails_activity,
            get_member_emails_activity,
            get_all_client_ids_activity,


            # broker emails
            send_broker_email_type1_activity,
            send_broker_email_type2_activity,

            # client emails
            send_client_email_type1_activity,
            send_client_email_type2_activity,
            send_client_email_type3_activity,

            # member emails
            send_member_email_type1_activity,
            send_member_email_type2_activity,
            send_member_email_type3_activity,
        ],
    )

    print("Worker started on task queue:", task_queue)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
