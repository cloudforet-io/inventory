import logging

from spaceone.core.service import *
from spaceone.inventory.model.record_model import Record
from spaceone.inventory.model.cloud_service_model import CloudService
from spaceone.inventory.manager.record_manager import RecordManager
from spaceone.inventory.manager.note_manager import NoteManager
from spaceone.inventory.manager.cloud_service_manager import CloudServiceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class NoteService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.note_mgr: NoteManager = self.locator.get_manager('NoteManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['record_id', 'note', 'domain_id'])
    def create(self, params):
        """Create record note

        Args:
            params (dict): {
                'record_id': 'str',
                'note': 'str',
                'domain_id': 'str'
            }

        Returns:
            note_vo (object)
        """

        user_id = self.transaction.get_meta('user_id')

        cloud_svc_mgr: CloudServiceManager = self.locator.get_manager('CloudServiceManager')
        record_mgr: RecordManager = self.locator.get_manager('RecordManager')
        record_vo: Record = record_mgr.get_record(params['record_id'], params['domain_id'])
        cloud_svc_vo: CloudService = cloud_svc_mgr.get_cloud_service(record_vo.cloud_service_id, params['domain_id'])

        params['cloud_service_id'] = record_vo.cloud_service_id
        params['project_id'] = cloud_svc_vo.project_id
        params['created_by'] = user_id

        return self.note_mgr.create_note(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['note_id', 'domain_id'])
    def update(self, params):
        """Update record note

        Args:
            params (dict): {
                'note_id': 'str',
                'note': 'dict',
                'domain_id': 'str'
            }

        Returns:
            note_vo (object)
        """
        
        note_id = params['note_id']
        domain_id = params['domain_id']

        note_vo = self.note_mgr.get_note(note_id, domain_id)

        # Check permission

        return self.note_mgr.update_note_by_vo(params, note_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['note_id', 'domain_id'])
    def delete(self, params):
        """Delete record note

        Args:
            params (dict): {
                'note_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        self.note_mgr.delete_note(params['note_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['note_id', 'domain_id'])
    def get(self, params):
        """ Get record note

        Args:
            params (dict): {
                'note_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            note_vo (object)
        """

        return self.note_mgr.get_note(params['note_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['note_id', 'record_id', 'cloud_service_id', 'created_by', 'domain_id', 'user_projects'])
    @append_keyword_filter(['note'])
    def list(self, params):
        """ List record notes

        Args:
            params (dict): {
                'note_id': 'str',
                'record_id': 'str',
                'cloud_service_id': 'str',
                'created_by': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta
            }

        Returns:
            note_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.note_mgr.list_notes(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['note'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.note_mgr.stat_notes(query)
