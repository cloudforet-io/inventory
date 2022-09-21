import logging
from datetime import datetime, timedelta
from spaceone.core.service import *
from spaceone.core import config
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.manager.cleanup_manager import CleanupManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager
from spaceone.inventory.manager.record_manager import RecordManager
from spaceone.inventory.manager.note_manager import NoteManager
from spaceone.inventory.manager.collector_manager.job_manager import JobManager
from spaceone.inventory.manager.collector_manager.job_task_manager import JobTaskManager
from spaceone.inventory.manager.cloud_service_tag_manager import CloudServiceTagManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@event_handler
class CleanupService(BaseService):

    @transaction
    @append_query_filter([])
    def list_domains(self, params):
        """
        Returns:
            response (dict): {
                'results': 'list',
                'total_count': 'int'
            }
        """
        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        query = params.get('query', {})
        return identity_mgr.list_domains(query)

    @transaction
    @check_required(['domain_id'])
    def update_job_state(self, params):
        """
        Args:
            params (dict): {
                'options': 'dict',
                'domain_id': 'str'
            }

        Based on domain's cleanup policy update  job.state

        Returns:
            None
        """
        domain_id = params['domain_id']

        # Get Cleanup Policy of domain
        # TODO: from domain config

        job_timeout = config.get_global('JOB_TIMEOUT', 2)  # hours

        policies = {
            'inventory.Job': {'TIMEOUT': job_timeout}
        }

        job_mgr: JobManager = self.locator.get_manager('JobManager')
        for resource_type, policy in policies.items():
            for status, hour in policy.items():
                _LOGGER.debug(f'[update_job_state] {resource_type}, {hour}, {status}, {domain_id}')
                job_mgr.update_job_status_by_hour(hour, status, domain_id)

    @transaction
    @check_required(['domain_id'])
    def terminate_jobs(self, params):
        """
        Args:
            params (dict): {
                'options': 'dict',
                'domain_id': 'str'
            }

        terminate old resources

        Returns:
            None
        """

        domain_id = params['domain_id']

        job_mgr: JobManager = self.locator.get_manager('JobManager')
        job_task_mgr: JobTaskManager = self.locator.get_manager('JobTaskManager')
        termination_time = config.get_global('JOB_TERMINATION_TIME', 30 * 2)  # days

        query = {
            'filter': [
                {
                    'k': 'created_at',
                    'v': datetime.utcnow() - timedelta(days=termination_time),
                    'o': 'lt'
                },
                {
                    'k': 'domain_id',
                    'v': domain_id,
                    'o': 'eq'
                }
            ]
        }

        job_vos, total_count = job_mgr.list_jobs(query)
        _LOGGER.info(f'[terminate_jobs] Terminate jobs: {str(total_count)}')
        job_vos.delete()

        job_task_vos, total_count = job_task_mgr.list(query)
        _LOGGER.info(f'[terminate_jobs] Terminate job tasks: {str(total_count)}')
        job_task_vos.delete()

    @transaction
    @check_required(['domain_id'])
    def delete_resources(self, params):
        """
        Args:
            params (dict): {
                'options': 'dict',
                'domain_id': 'str'
            }

        Based on domain's delete policy, delete resources

        Returns:
            None
        """

        domain_id = params['domain_id']

        exclude_domains = config.get_global('DELETE_EXCLUDE_DOMAINS', [])

        if domain_id not in exclude_domains:
            # Get Delete Policy of domain
            # TODO: from domain config

            # policies = self._get_domain_config(state, domain_id)

            policies = config.get_global('DEFAULT_DELETE_POLICIES', {})
            _LOGGER.debug(f'[delete_resources] {policies}')

            cleanup_mgr: CleanupManager = self.locator.get_manager('CleanupManager')
            for resource_type, hour in policies.items():
                try:
                    _LOGGER.debug(f'[delete_resources] {resource_type}, {hour}, {domain_id}')
                    deleted_count = cleanup_mgr.delete_resources_by_policy(resource_type, hour, domain_id)
                    _LOGGER.debug(f'[delete_resources] number of deleted count: {deleted_count}')

                    # TODO: event notification
                except Exception as e:
                    _LOGGER.error(f'[delete_resources] {e}')
        else:
            _LOGGER.debug(f'[delete_resources] skip domain: {domain_id}')

    @transaction
    @check_required(['domain_id'])
    def terminate_resources(self, params):
        """
        Args:
            params (dict): {
                'options': 'dict',
                'domain_id': 'str'
            }

        terminate old resources

        Returns:
            None
        """

        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')
        record_mgr: RecordManager = self.locator.get_manager('RecordManager')
        note_mgr: NoteManager = self.locator.get_manager('NoteManager')
        tag_mgr: CloudServiceTagManager = self.locator.get_manager('CloudServiceTagManager')

        domain_id = params['domain_id']

        termination_time = config.get_global('RESOURCE_TERMINATION_TIME', 30 * 6)  # days

        query = {
            'filter': [
                {
                    'k': 'deleted_at',
                    'v': datetime.utcnow() - timedelta(days=termination_time),
                    'o': 'lt'
                },
                {
                    'k': 'state',
                    'v': 'DELETED',
                    'o': 'eq'
                },
                {
                    'k': 'domain_id',
                    'v': domain_id,
                    'o': 'eq'
                }
            ]
        }

        cloud_svc_vos, total_count = cloud_svc_mgr.list_cloud_services(query)
        _LOGGER.info(f'[terminate_resources] Terminate cloud services: {str(total_count)}')
        for cloud_svc_vo in cloud_svc_vos:
            cloud_service_id = cloud_svc_vo.cloud_service_id
            domain_id = cloud_svc_vo.domain_id
            _LOGGER.info(f'[terminate_resources] Terminate cloud service / record / note: {cloud_service_id}')

            # Cascade Delete Records
            record_vos = record_mgr.filter_records(cloud_service_id=cloud_service_id, domain_id=domain_id)
            record_vos.delete()

            # Cascade Delete Notes
            note_vos = note_mgr.filter_notes(cloud_service_id=cloud_service_id, domain_id=domain_id)
            note_vos.delete()

            # Cascade Delete CloudServiceTags
            cloud_svc_tags_vos = tag_mgr.filter_cloud_svc_tags(cloud_service_id=cloud_service_id, domain_id=domain_id)
            cloud_svc_tags_vos.delete()

        cloud_svc_vos.delete()
