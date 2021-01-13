import logging

from spaceone.core.service import *
from spaceone.inventory.manager.cleanup_manager import CleanupManager
from spaceone.inventory.manager.config_manager import ConfigManager

_LOGGER = logging.getLogger(__name__)

# define as Domain config variable
DEFAULT_POLICY = {
    'garbage_collection.inventory.DISCONNECTED': {
            'inventory.Server': 24,
            'inventory.CloudService': 24
        },
    'garbage_collection.inventory.DELETE': {
            'inventory.Server': 48,
            'inventory.CloudService': 48
    }
}

JOB_TIMEOUT = 2

@authentication_handler
@authorization_handler
@event_handler
class CleanupService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)

    @transaction
    @append_query_filter([])
    def list_domains(self, params):
        """
        Returns:
            results (list)
            total_count (int)
        """
        mgr = self.locator.get_manager('CleanupManager')
        query = params.get('query', {})
        result = mgr.list_domains(query)
        return result

    @transaction
    @check_required(['domain_id'])
    def update_collection_state(self, params):
        """
        Args:
            params (dict): {
                'options': {},
                'domain_id': str
            }

        Based on domain's cleanup policy update  collection.state

        Returns:
            values (list) : 'list of updated resources'

        """
        domain_id = params['domain_id']
        # Get Cleanup Policy of domain
        # TODO: from domain config
        state = 'DISCONNECTED'
        policies = self._get_domain_config(state, domain_id)
        _LOGGER.debug(f'[update_collection_state] policies = {policies}')
        #policies = {
        #            'inventory.Server?provider=aws&data.aws.lifecycle=spot': 12,
        #            'inventory.Server?provider=aws&data.auto_scaling_group!=': 0,
        #            'inventory.Server': 24,
        #            'inventory.CloudService': 24
        #            }

        mgr = self.locator.get_manager('CleanupManager')
        for resource_type, hour in policies.items():
            try:
                _LOGGER.debug(f'[update_collection_state] {resource_type}, {hour}, {state}, {domain_id}')
                mgr.update_collection_state(resource_type, hour, state, domain_id)
            except Exception as e:
                _LOGGER.error(f'[update_collection_state] {resource_type}, {hour}')
                _LOGGER.error(f'[update_collection_state] {e}')

    @transaction
    @check_required(['domain_id'])
    def update_job_state(self, params):
        """
        Args:
            params (dict): {
                'options': {},
                'domain_id': str
            }

        Based on domain's cleanup policy update  job.state

        Returns:
        """
        domain_id = params['domain_id']
        # Get Cleanup Policy of domain
        # TODO: from domain config
        policies = {'inventory.Job': {'TIMEOUT': JOB_TIMEOUT}}

        mgr = self.locator.get_manager('JobManager')
        for resource_type, policy in policies.items():
            for status, hour in policy.items():
                _LOGGER.debug(f'[update_job_state] {resource_type}, {hour}, {status}, {domain_id}')
                mgr.update_job_status_by_hour(hour, status, domain_id)

    @transaction
    @check_required(['domain_id'])
    def delete_resources(self, params):
        """
        Args:
            params (dict): {
                'options': {},
                'domain_id': str
                }

        Based on domain's delete policy, delete resources

        Returns:
        """
        domain_id = params['domain_id']
        # Get Delete Policy of domain
        # TODO: from domain config
        state = 'DELETE'
        policies = self._get_domain_config(state, domain_id)
        _LOGGER.debug(f'[delete_resources] {policies}')
        #policies = {'inventory.Server': 48,
        #            'inventory.CloudService': 48
        #            }

        mgr = self.locator.get_manager('CleanupManager')
        for resource_type, hour in policies.items():
            try:
                _LOGGER.debug(f'[delete_resources] {resource_type}, {hour}, {state}, {domain_id}')
                deleted_resources, total_count = mgr.delete_resources_by_policy(resource_type, hour, state, domain_id)
                _LOGGER.debug(f'[delete_resources] number of deleted count: {total_count}')
                _LOGGER.debug(f'[delete_resources] resources: {deleted_resources}')
                # TODO: event notification
            except Exception as e:
                _LOGGER.error(f'[delete_resources] {resource_type}, {hour}')
                _LOGGER.error(f'[delete_resources] {e}')

    def _get_domain_config(self, name, domain_id):
        """ Get domain config with name
        """
        try:
            cfg_mgr = self.locator.get_manager('ConfigManager')
            policy_name = f'garbage_collection.inventory.{name}'

            return cfg_mgr.get_domain_config(policy_name, domain_id)
        except Exception as e:
            _LOGGER.debug(f'[_get_domain_config] fail to get config {name} {domain_id}')
            return DEFAULT_POLICY[policy_name]
