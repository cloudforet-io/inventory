import logging
from spaceone.core.service import *
from spaceone.core import utils
from spaceone.inventory.error import *
from spaceone.inventory.manager.resource_group_manager import ResourceGroupManager
from spaceone.inventory.manager.identity_manager import IdentityManager

_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ['resource_group_id', 'name']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ResourceGroupService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.resource_group_mgr: ResourceGroupManager = self.locator.get_manager('ResourceGroupManager')

    @transaction(append_meta={
        'authorization.scope': 'PROJECT',
        'authorization.require_project_id': True
    })
    @check_required(['name', 'resources', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'resources': 'list',
                    'project_id': 'str',
                    'options': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            resource_group_vo (object)
        """

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        return self.resource_group_mgr.create_resource_group(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['resource_group_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'resource_group_id': 'str',
                    'name': 'str',
                    'resources': 'list',
                    'project_id': 'str',
                    'release_project': 'bool',
                    'options': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            resource_group_vo (object)

        """

        if 'tags' in params:
            if isinstance(params['tags'], list):
                params['tags'] = utils.tags_to_dict(params['tags'])

        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        project_id = params.get('project_id', self.transaction.get_meta('secret.project_id'))
        release_project = params.get('release_project', False)

        if release_project:
            params['project_id'] = None
        elif project_id:
            identity_mgr.get_project(project_id, params['domain_id'])
            params['project_id'] = project_id

        rg_vo = self.resource_group_mgr.get_resource_group(params['resource_group_id'], params['domain_id'])
        return self.resource_group_mgr.update_resource_group_by_vo(params, rg_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['resource_group_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'resource_group_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        rg_vo = self.resource_group_mgr.get_resource_group(params['resource_group_id'], params['domain_id'])
        self.resource_group_mgr.delete_resource_group_by_vo(rg_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['resource_group_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'resource_group_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            resource_group_vo (object)

        """

        return self.resource_group_mgr.get_resource_group(params['resource_group_id'], params['domain_id'],
                                                          params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(['resource_group_id', 'name', 'project_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def list(self, params):
        """
        Args:
            params (dict): {
                    'resource_group_id': 'str',
                    'name': 'str',
                    'project_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)',
                    'user_projects': 'list', // from meta
                }

        Returns:
            results (list)
            total_count (int)

        """
        return self.resource_group_mgr.list_resource_groups(params.get('query', {}))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(_KEYWORD_FILTER)
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
        return self.resource_group_mgr.stat_resource_groups(query)
