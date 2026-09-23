import asyncio
from email.message import EmailMessage
from email.utils import formatdate

import aiosmtplib
from notification_service.retry import TemporaryError, PermanentError


def build_message(settings, recipient, rendered, message_id):
    message = EmailMessage()
    message['From'], message['To'] = str(settings.smtp_from), str(recipient.email)
    message['Subject'], message['Message-ID'] = rendered.subject, message_id
    message['Date'] = formatdate(localtime=False)
    message.set_content(rendered.text)
    message.add_alternative(rendered.html, subtype='html')
    return message


class SMTPClient:
    def __init__(self, settings):
        self.settings = settings

    async def send(self, recipient, rendered, message_id):
        s = self.settings
        try:
            async with asyncio.timeout(30):
                await aiosmtplib.send(build_message(s, recipient, rendered, message_id),
                    hostname=s.smtp_host, port=s.smtp_port, timeout=30,
                    username=s.smtp_username,
                    password=s.smtp_password.get_secret_value() if s.smtp_password else None,
                    start_tls=s.smtp_start_tls, use_tls=s.smtp_use_tls)
        except aiosmtplib.errors.SMTPRecipientsRefused as error:
            code = error.recipients[0].code
            raise (TemporaryError('smtp_temporary') if code < 500 else PermanentError('smtp_rejected')) from error
        except aiosmtplib.errors.SMTPResponseException as error:
            raise (TemporaryError('smtp_temporary') if error.code < 500 else PermanentError('smtp_rejected')) from error
        except (OSError, TimeoutError, aiosmtplib.errors.SMTPException) as error:
            raise TemporaryError('smtp_network') from error
