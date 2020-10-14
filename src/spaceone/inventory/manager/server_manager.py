import logging

from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.model.server_model import Server
from spaceone.inventory.lib.resource_manager import ResourceManager
from spaceone.inventory.error import *

_LOGGER = logging.getLogger(__name__)


class ServerManager(BaseManager, ResourceManager):

    resource_keys = ['server_id']
    query_method = 'list_servers'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server_model: Server = self.locator.get_model('Server')

    def create_server(self, params):
        def _rollback(server_vo):
            _LOGGER.info(f'[ROLLBACK] Delete Server : {server_vo.name} ({server_vo.server_id})')
            server_vo.terminate()

        server_vo: Server = self.server_model.create(params)
        self.transaction.add_rollback(_rollback, server_vo)

        return server_vo

    def update_server(self, params):
        server_vo: Server = self.get_server(params['server_id'], params['domain_id'])
        return self.update_server_by_vo(params, server_vo)

    def update_server_by_vo(self, params, server_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[ROLLBACK] Revert Server Data : {old_data["name"]} ({old_data["server_id"]})')
            server_vo.update(old_data)

        self.transaction.add_rollback(_rollback, server_vo.to_dict())
        return server_vo.update(params)

    def delete_server(self, server_id, domain_id):
        server_vo: Server = self.get_server(server_id, domain_id)
        server_vo.delete()

    def get_server(self, server_id, domain_id, only=None):
        return self.server_model.get(server_id=server_id, domain_id=domain_id, only=only)

    def list_servers(self, query):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.server_model.query(**query)

    def stat_servers(self, query):
        # Append Query for DELETED filter (Temporary Logic)
        query = self._append_state_query(query)
        return self.server_model.stat(**query)

    @staticmethod
    def _append_state_query(query):
        state_default_filter = {
            'key': 'state',
            'value': 'DELETED',
            'operator': 'not'
        }

        show_deleted_resource = False
        for condition in query.get('filter', []):
            key = condition.get('k', condition.get('key'))
            value = condition.get('v', condition.get('value'))
            operator = condition.get('o', condition.get('operator'))

            if key == 'state':
                if operator == 'eq' and value == 'DELETED':
                    show_deleted_resource = True
                elif operator in ['in', 'contain_in'] and 'DELETED' in value:
                    show_deleted_resource = True

        if not show_deleted_resource:
            query['filter'] = query.get('filter', [])
            query['filter'].append(state_default_filter)

        return query
