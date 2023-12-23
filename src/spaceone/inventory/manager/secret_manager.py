import logging

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector

_LOGGER = logging.getLogger(__name__)


class SecretManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_connector: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="secret"
        )

    def get_secret_data(self, secret_id: str, domain_id: str = None) -> dict:
        return self.secret_connector.dispatch(
            "Secret.get_data",
            {"secret_id": secret_id, "domain_id": domain_id},
            token=config.get_global("TOKEN"),
        )

    def get_secret(
        self, secret_id: str, token: str = None, x_domain_id: str = None
    ) -> dict:
        return self.secret_connector.dispatch(
            "Secret.get", {"secret_id": secret_id}, token=token, x_domain_id=x_domain_id
        )

    def list_secrets(
        self, query: dict, token: str = None, x_domain_id: str = None
    ) -> dict:
        return self.secret_connector.dispatch(
            "Secret.list", {"query": query}, token=token, x_domain_id=x_domain_id
        )
