import logging

from spaceone.core.service import *
from spaceone.inventory.manager.cleanup_manager import CleanupManager

_LOGGER = logging.getLogger(__name__)


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
        policies = {'inventory.Server': {'DISCONNECTED': 24},
                    'inventory.CloudService': {'DISCONNECTED': 24}}

        mgr = self.locator.get_manager('CleanupManager')
        for resource_type, policy in policies.items():
            for state, hour in policy.items():
                _LOGGER.debug(f'[update_collection_state] {resource_type}, {hour}, {state}, {domain_id}')
                mgr.update_collection_state(resource_type, hour, state, domain_id)

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
        policies = {'inventory.Job': {'FAILURE': 1}}

        mgr = self.locator.get_manager('JobManager')
        for resource_type, policy in policies.items():
            for state, hour in policy.items():
                _LOGGER.debug(f'[update_job_state] {resource_type}, {hour}, {state}, {domain_id}')
                mgr.update_job_state_by_hour(hour, state, domain_id)
