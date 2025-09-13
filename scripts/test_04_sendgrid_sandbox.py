# scripts/test_04_sendgrid_sandbox.py
from app.activities.email import send_broker_email
print(
    send_broker_email(
        "tanisharaaj2167@gmail.com",
        {
            "client_id": 1,
            "broker_id": 101,
            "brand_name": "Example Brand",
            "app_name": "Example App",
            "appstore_link": "https://apps.apple.com/app/id000000",
            "playstore_link": "https://play.google.com/store/apps/details?id=example",
            "website_portal": "https://example.com/portal",
            "cta_url": "https://example.com/action"
        },
    )
)
