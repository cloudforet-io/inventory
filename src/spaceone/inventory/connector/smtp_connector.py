import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from spaceone.core.connector import BaseConnector
from spaceone.inventory.error.smtp import *


__all__ = ['SMTPConnector']

_LOGGER = logging.getLogger(__name__)


class SMTPConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.smtp = None
        host = self.config.get('host')
        port = self.config.get('port')
        user = self.config.get('user')
        password = self.config.get('password')
        self.from_email = self.config.get('from_email')
        self.set_smtp(host, port, user, password)

    def set_smtp(self, host, port, user, password):
        try:
            self.smtp = smtplib.SMTP(host, port)
            self.smtp.connect(host, port)
            self.smtp.ehlo()
            self.smtp.starttls()
            self.smtp.login(user, password)
        except Exception as e:
            _LOGGER.error(f'[set_smtp] set smtp failed : Please check smtp config {e}')
            raise ERROR_SMTP_CONNECTION_FAILED()

    def send_email(self, to_emails, subject, contents):
        multipart_msg = MIMEMultipart("alternative")

        multipart_msg["Subject"] = subject
        multipart_msg["From"] = self.from_email
        multipart_msg["To"] = to_emails

        multipart_msg.attach(MIMEText(contents, 'html'))

        self.smtp.sendmail(self.from_email, to_emails.split(','), multipart_msg.as_string())

    def quit_smtp(self):
        self.smtp.quit()
