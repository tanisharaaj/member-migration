from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from app.settings import settings


def _send_via_sendgrid(to_email: str, dynamic_data: dict, template_id: str):
    message = Mail(
        from_email=settings.SENDGRID_FROM_EMAIL,   
        to_emails=to_email,
    )
    message.template_id = template_id
    message.dynamic_template_data = dynamic_data
    sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
    response = sg.send(message)
    return response.status_code



# --- Broker templates ---
def send_broker_email_type1(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_BROKER_TEMPLATE_1)

def send_broker_email_type2(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_BROKER_TEMPLATE_2)


# --- Client templates ---
def send_client_email_type1(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_CLIENT_TEMPLATE_1)

def send_client_email_type2(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_CLIENT_TEMPLATE_2)
    
def send_client_email_type3(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_CLIENT_TEMPLATE_3)


# --- Member templates ---
def send_member_email_type1(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_MEMBER_TEMPLATE_1)

def send_member_email_type2(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_MEMBER_TEMPLATE_2)


def send_member_email_type3(to_email: str, dynamic_data: dict):
    return _send_via_sendgrid(to_email, dynamic_data, settings.SENDGRID_MEMBER_TEMPLATE_3)