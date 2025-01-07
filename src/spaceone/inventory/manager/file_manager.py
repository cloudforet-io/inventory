import logging
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.inventory.connector.file_upload_connector import (
    FileUploadConnector,
    FilesUploadConnector,
)


_LOGGER = logging.getLogger(__name__)

_CONNECTOR_MAP = {"FILES": FilesUploadConnector}


class FileManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.file_mgr_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="file_manager"
        )

    def upload_user_file(self, file_path: str) -> dict:
        connector_name = _CONNECTOR_MAP.get("FILES", "FILES")
        file_upload_connector: FileUploadConnector = self.locator.get_connector(
            connector_name
        )
        return file_upload_connector.upload_user_file(file_path)
