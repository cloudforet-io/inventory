import os
import tempfile
import logging
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

from spaceone.inventory.manager.export_manager import ExportManager
from spaceone.inventory.connector.smtp_connector import SMTPConnector

_LOGGER = logging.getLogger(__name__)

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), f"../../template")
JINJA_ENV = Environment(
    loader=FileSystemLoader(searchpath=TEMPLATE_PATH), autoescape=select_autoescape()
)


class EmailManager(ExportManager):
    def export(
        self, export_options: dict, domain_id: str, workspace_id: str = None, **kwargs
    ) -> None:
        name = kwargs.get("name", "Cloud Service Report")
        target = kwargs.get("target", {})
        emails = target.get("emails", [])
        created_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        with tempfile.TemporaryDirectory() as temp_dir:
            self._file_dir = temp_dir
            self._file_path = f"{self._file_dir}/{self._file_name}"
            self.make_file(export_options)
            response = self.upload_file(domain_id, workspace_id)

            download_url = response["download_url"]
            subject = "[SpaceONE] The Cloud Service Report File is ready to download"

            for email in emails:
                template = JINJA_ENV.get_template("report_download_en.html")
                email_contents = template.render(
                    user_name=email,
                    report_name=name,
                    created_time=created_time,
                    download_link=download_url,
                )

                self.send_email(email, subject, email_contents)

    def send_email(self, email: str, subject: str, contents: str) -> None:
        smtp_connector: SMTPConnector = self.locator.get_connector("SMTPConnector")
        smtp_connector.send_email(email, subject, contents)
