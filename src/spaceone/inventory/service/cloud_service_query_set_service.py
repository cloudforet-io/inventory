import logging

from spaceone.core import config
from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet
from spaceone.inventory.manager.cloud_service_query_set_manager import CloudServiceQuerySetManager
from spaceone.inventory.manager.identity_manager import IdentityManager


_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ['query_set_id', 'name']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceQuerySetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_query_set_mgr: CloudServiceQuerySetManager = self.locator.get_manager('CloudServiceQuerySetManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'query_options', 'query_options.fields', 'provider', 'cloud_service_group',
                     'cloud_service_type', 'domain_id'])
    def create(self, params):
        """ Create Cloud Service Query Set
        Args:
            params (dict): {
                    'name': 'str',
                    'query_options': 'dict',
                    'unit': 'dict',
                    'provider': 'str',
                    'cloud_service_group': 'str',
                    'cloud_service_type': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_query_set_vo (object)

        """

        params['query_type'] = 'CUSTOM'

        return self.cloud_svc_query_set_mgr.create_cloud_service_query_set(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def update(self, params):
        """ Update Cloud Service Query Set
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'name': 'str',
                    'query_options': 'dict',
                    'unit': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_query_set_vo (object)

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        if cloud_svc_query_set_vo.query_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo(params, cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def delete(self, params):
        """ Delete Cloud Service Query Set
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        if cloud_svc_query_set_vo.query_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        self.cloud_svc_query_set_mgr.delete_cloud_service_query_set_by_vo(cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def run(self, params):
        """ Run Query Set Manually and Save Results
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        self.cloud_svc_query_set_mgr.run_cloud_service_query_set(cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def test(self, params):
        """ Test Query Set Manually
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            values (list) : 'list of analyze data'

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        return self.cloud_svc_query_set_mgr.test_cloud_service_query_set(cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def enable(self, params):
        """ Enable Cloud Service Query Set
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_query_set_vo (object)

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        if cloud_svc_query_set_vo.query_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo({'state': 'ENABLED'},
                                                                                 cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def disable(self, params):
        """ Disable Cloud Service Query Set
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_query_set_vo (object)

        """

        cloud_svc_query_set_vo: CloudServiceQuerySet = \
            self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'], params['domain_id'])

        if cloud_svc_query_set_vo.query_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_QUERY_TYPE()

        return self.cloud_svc_query_set_mgr.update_cloud_service_query_set_by_vo({'state': 'DISABLED'},
                                                                                 cloud_svc_query_set_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query_set_id', 'domain_id'])
    def get(self, params):
        """ Get Cloud Service Query Set
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            cloud_service_type_vo (object)

        """

        return self.cloud_svc_query_set_mgr.get_cloud_service_query_set(params['query_set_id'],
                                                                        params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['query_set_id', 'name', 'state', 'query_type', 'provider', 'cloud_service_group',
                          'cloud_service_type', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """ List Cloud Service Query Sets
        Args:
            params (dict): {
                    'query_set_id': 'str',
                    'name': 'str',
                    'state': 'str',
                    'query_type': 'str',
                    'provider': 'str',
                    'cloud_service_group': 'str',
                    'cloud_service_type': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """
        query = params.get('query', {})
        return self.cloud_svc_query_set_mgr.list_cloud_service_query_sets(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """ Get Cloud Service Query Set Statistics
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.cloud_svc_query_set_mgr.stat_cloud_service_query_sets(query)

    @transaction(append_meta={'authorization.scope': 'SYSTEM'})
    @check_required(['domain_id'])
    def run_query_sets_by_domain(self, params):
        """ Run cloud service query sets by domain_id

        Args:
            params (dict): {
                'domain_id': 'str'
            }

        Returns:
            None
        """

        domain_id = params['domain_id']
        query_set_vos = self.cloud_svc_query_set_mgr.filter_cloud_service_query_sets(domain_id=domain_id)
        for query_set_vo in query_set_vos:
            self.cloud_svc_query_set_mgr.run_cloud_service_query_set(query_set_vo)

    @transaction(append_meta={'authorization.scope': 'SYSTEM'})
    def run_all_query_sets(self, params):
        """ Run all cloud service query sets

        Args:
            params (dict): {}

        Returns:
            None
        """

        system_token = config.get_global('TOKEN')

        for domain_info in self._get_all_domains_info(system_token):
            domain_id = domain_info['domain_id']
            try:
                self.cloud_svc_query_set_mgr.push_task(domain_id, system_token)
            except Exception as e:
                _LOGGER.error(f'[run_query_sets_by_domain] query error({domain_id}): {e}', exc_info=True)

    def _get_all_domains_info(self, system_token):
        identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager', token=system_token)
        response = identity_mgr.list_domains({
            'only': ['domain_id'],
            'filter': [{'k': 'state', 'v': 'ENABLED', 'o': 'eq'}]
        })

        return response.get('results', [])

    def _list_query_sets_by_domain(self, domain_id):
        return self.cloud_svc_query_set_mgr.filter_cloud_service_query_sets(domain_id=domain_id)
