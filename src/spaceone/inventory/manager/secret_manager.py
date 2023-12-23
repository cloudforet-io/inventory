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

    def get_secret(self, secret_id: str) -> dict:
        return self.secret_connector.dispatch("Secret.get", {"secret_id": secret_id})

    def list_secrets(self, query: dict) -> dict:
        return self.secret_connector.dispatch("Secret.list", {"query": query})

    def get_secret_data(self, secret_id: str, domain_id: str = None) -> dict:
        return self.secret_connector.dispatch(
            "Secret.get_data",
            {"secret_id": secret_id, "domain_id": domain_id},
        )

    def get_secret_by_system_token(self, secret_id: str, domain_id: str) -> dict:
        system_token = config.get_global("TOKEN")
        return self.secret_connector.dispatch(
            "Secret.get",
            {"secret_id": secret_id},
            token=system_token,
            x_domain_id=domain_id,
        )

    def list_secrets_by_system_token(self, query: dict, domain_id: str) -> dict:
        system_token = config.get_global("TOKEN")
        return self.secret_connector.dispatch(
            "Secret.list",
            {"query": query},
            token=system_token,
            x_domain_id=domain_id,
        )
