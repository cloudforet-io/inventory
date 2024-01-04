import logging
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.inventory.connector.file_upload_connector import (
    FileUploadConnector,
    AWSS3UploadConnector,
)

_LOGGER = logging.getLogger(__name__)

_CONNECTOR_MAP = {"AWS_S3": AWSS3UploadConnector}


class FileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.file_mgr_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="file_manager"
        )

    def add_file(self, params: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.file_mgr_conn.dispatch(
                "File.add", params, x_domain_id=domain_id
            )
        else:
            return self.file_mgr_conn.dispatch("File.add", params)

    def get_download_url(self, file_id: str, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.file_mgr_conn.dispatch(
                "File.get_download_url",
                {"file_id": file_id},
                x_domain_id=domain_id,
            )
        else:
            return self.file_mgr_conn.dispatch(
                "File.get_download_url", {"file_id": file_id}
            )

    def upload_file(
        self, file_path: str, url: str, options: dict, storage_type: str = "AWS_S3"
    ):
        connector_name = _CONNECTOR_MAP.get(storage_type, "AWS_S3")
        file_upload_connector: FileUploadConnector = self.locator.get_connector(
            connector_name
        )
        file_upload_connector.upload_file(file_path, url, options)
