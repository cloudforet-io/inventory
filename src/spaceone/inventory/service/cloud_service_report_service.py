import logging

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.inventory.model.cloud_service_report_model import CloudServiceReport
from spaceone.inventory.manager.cloud_service_report_manager import CloudServiceReportManager


_LOGGER = logging.getLogger(__name__)
_KEYWORD_FILTER = ['report_id', 'name']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CloudServiceReportService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud_svc_report_mgr: CloudServiceReportManager = self.locator.get_manager('CloudServiceReportManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'options', 'schedule', 'schedule.state', 'target', 'domain_id'])
    def create(self, params):
        """ Create Cloud Service Report
        Args:
            params (dict): {
                    'name': 'str',
                    'options': 'dict',
                    'file_format': 'str',
                    'schedule': 'dict',
                    'target': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_report_vo (object)

        """

        return self.cloud_svc_report_mgr.create_cloud_service_report(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['report_id', 'domain_id'])
    def update(self, params):
        """ Update Cloud Service Report
        Args:
            params (dict): {
                    'report_id': 'str',
                    'name': 'str',
                    'options': 'dict',
                    'file_format': 'str',
                    'schedule': 'dict',
                    'target': 'dict',
                    'tags': 'dict',
                    'domain_id': 'str'
                }

        Returns:
            cloud_service_report_vo (object)

        """

        cloud_svc_report_vo: CloudServiceReport = \
            self.cloud_svc_report_mgr.get_cloud_service_report(params['report_id'], params['domain_id'])

        return self.cloud_svc_report_mgr.update_cloud_service_report_by_vo(params, cloud_svc_report_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['report_id', 'domain_id'])
    def delete(self, params):
        """ Delete Cloud Service Report
        Args:
            params (dict): {
                    'report_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_report_vo: CloudServiceReport = \
            self.cloud_svc_report_mgr.get_cloud_service_report(params['report_id'], params['domain_id'])

        self.cloud_svc_report_mgr.delete_cloud_service_report_by_vo(cloud_svc_report_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['report_id', 'domain_id'])
    def send(self, params):
        """ Send Report Manually
        Args:
            params (dict): {
                    'report_id': 'str',
                    'domain_id': 'str'
                }

        Returns:
            None

        """

        cloud_svc_report_vo: CloudServiceReport = \
            self.cloud_svc_report_mgr.get_cloud_service_report(params['report_id'], params['domain_id'])

        self.cloud_svc_report_mgr.send_cloud_service_report(cloud_svc_report_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['report_id', 'domain_id'])
    def get(self, params):
        """ Get Cloud Service Report
        Args:
            params (dict): {
                    'report_id': 'str',
                    'domain_id': 'str',
                    'only': 'list'
                }

        Returns:
            cloud_service_type_vo (object)

        """

        return self.cloud_svc_report_mgr.get_cloud_service_report(params['report_id'],
                                                                  params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['report_id', 'name', 'domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    @set_query_page_limit(1000)
    def list(self, params):
        """ List Cloud Service Reports
        Args:
            params (dict): {
                    'report_id': 'str',
                    'name': 'str',
                    'domain_id': 'str',
                    'query': 'dict (spaceone.api.core.v1.Query)'
                }

        Returns:
            results (list)
            total_count (int)

        """
        query = params.get('query', {})
        return self.cloud_svc_report_mgr.list_cloud_service_reports(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(_KEYWORD_FILTER)
    def stat(self, params):
        """ Get Cloud Service Report Statistics
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        query = params.get('query', {})
        return self.cloud_svc_report_mgr.stat_cloud_service_reports(query)
