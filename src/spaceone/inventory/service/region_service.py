import logging
from spaceone.core.service import *
from spaceone.inventory.manager.region_manager import RegionManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class RegionService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.region_mgr: RegionManager = self.locator.get_manager('RegionManager')

    @transaction
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)

        """

        region_mgr: RegionManager = self.locator.get_manager('RegionManager')
        return region_mgr.create_region(params)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def update(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'name': 'str',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            region_vo (object)

        """

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])
        return self.region_mgr.update_region_by_vo(params, region_vo)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def delete(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        region_vo = self.region_mgr.get_region(params['region_id'], params['domain_id'])
        self.region_mgr.delete_region_by_vo(region_vo)

    @transaction
    @check_required(['region_id', 'domain_id'])
    def get(self, params):
        """
        Args:
            params (dict): {
                    'region_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            region_vo (object)

        """

        return self.region_mgr.get_region(params['region_id'], params['domain_id'], params.get('only'))

    @transaction
    @check_required(['domain_id'])
    @append_query_filter(['region_id', 'name', 'domain_id'])
    @append_keyword_filter(['region_id', 'name'])
    def list(self, params):
        """
        Args:
            params (dict): {
                    'name': 'str',
                    'region_id': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """

        return self.region_mgr.list_regions(params.get('query', {}))

    @transaction
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.region_mgr.stat_regions(query)
