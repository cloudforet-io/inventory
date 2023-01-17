import logging
import fnmatch

from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.model.collector_rule_model import CollectorRule

_LOGGER = logging.getLogger(__name__)

_SUPPORTED_CONDITION_KEYS = ['provider', 'region_code', 'product', 'account', 'usage_type', 'resource_group',
                             'resource', 'tags.<key>', 'additional_info.<key>']
_SUPPORTED_CONDITION_OPERATORS = ['eq', 'contain', 'not', 'not_contain']


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CollectorRuleService(BaseService):

    def __init__(self, metadata):
        super().__init__(metadata)
        self.collector_rule_mgr: CollectorRuleManager = self.locator.get_manager('CollectorRuleManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_id', 'conditions_policy', 'actions', 'domain_id'])
    def create(self, params):
        """Create Collector rule

        Args:
            params (dict): {
                'collector_id': 'str',
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'str',
                'actions': 'dict',
                'options': 'dict',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            collector_rule_vo (object)
        """

        domain_id = params['domain_id']
        collector_id = params['collector_id']
        conditions = params.get('conditions', [])
        conditions_policy = params['conditions_policy']
        actions = params['actions']
        rule_type = params.get('rule_type', 'CUSTOM')

        if conditions_policy == 'ALWAYS':
            params['conditions'] = []
        else:
            if len(conditions) == 0:
                raise ERROR_REQUIRED_PARAMETER(key='conditions')
            else:
                self._check_conditions(conditions)

        self._check_actions(actions, domain_id)

        collector_mgr: CollectorManager = self.locator.get_manager('CollectorManager')
        collector_vo = collector_mgr.get_collector(collector_id, domain_id)

        params['collector'] = collector_vo
        params['order'] = self._get_highest_order(collector_id, rule_type, domain_id) + 1

        return self.collector_rule_mgr.create_collector_rule(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_rule_id', 'domain_id'])
    def update(self, params):
        """ Update collector rule
        Args:
            params (dict): {
                'collector_rule_id': 'str',
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'list',
                'actions': 'dict',
                'options': 'dict'
                'tags': 'dict'
                'domain_id': 'str'
            }

        Returns:
            collector_rule_vo (object)
        """
        collector_rule_id = params['collector_rule_id']
        domain_id = params['domain_id']
        conditions_policy = params.get('conditions_policy')
        conditions = params.get('conditions', [])

        collector_rule_vo = self.collector_rule_mgr.get_collector_rule(collector_rule_id, domain_id)

        if collector_rule_vo.rule_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_TO_UPDATE_RULE()

        if conditions_policy:
            if conditions_policy == 'ALWAYS':
                params['conditions'] = []
            else:
                if len(conditions) == 0:
                    raise ERROR_REQUIRED_PARAMETER(key='conditions')
                else:
                    self._check_conditions(conditions)

        if 'actions' in params:
            self._check_actions(params['actions'], domain_id)

        return self.collector_rule_mgr.update_collector_rule_by_vo(params, collector_rule_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_rule_id', 'order', 'domain_id'])
    def change_order(self, params):
        """ Change collector rule's order

        Args:
            params (dict): {
                'collector_rule_id': 'str',
                'order': 'int',
                'domain_id': 'str'
            }

        Returns:
            collector_rule_vo (object)
        """
        collector_rule_id = params['collector_rule_id']
        order = params['order']
        domain_id = params['domain_id']

        self._check_order(order)

        target_collector_rule_vo: CollectorRule = self.collector_rule_mgr.get_collector_rule(collector_rule_id,
                                                                                             domain_id)

        if target_collector_rule_vo.rule_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_TO_CHANGE_ORDER()

        if target_collector_rule_vo.order == order:
            return target_collector_rule_vo

        highest_order = self._get_highest_order(target_collector_rule_vo.collector_id,
                                                target_collector_rule_vo.rule_type,
                                                target_collector_rule_vo.domain_id)

        if order > highest_order:
            raise ERROR_INVALID_PARAMETER(key='order',
                                          reason=f'There is no collector rules greater than the {str(order)} order.')

        collector_rule_vos = self._get_all_collector_rules(target_collector_rule_vo.collector_id,
                                                           target_collector_rule_vo.rule_type,
                                                           target_collector_rule_vo.domain_id,
                                                           target_collector_rule_vo.collector_rule_id)

        collector_rule_vos.insert(order - 1, target_collector_rule_vo)

        i = 0
        for collector_rule_vo in collector_rule_vos:
            if target_collector_rule_vo != collector_rule_vo:
                self.collector_rule_mgr.update_collector_rule_by_vo({'order': i + 1}, collector_rule_vo)

            i += 1

        return self.collector_rule_mgr.update_collector_rule_by_vo({'order': order}, target_collector_rule_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_rule_id', 'domain_id'])
    def delete(self, params):
        """ Delete collector rule

        Args:
            params (dict): {
                'collector_rule_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """
        collector_rule_id = params['collector_rule_id']
        domain_id = params['domain_id']

        collector_rule_vo: CollectorRule = self.collector_rule_mgr.get_collector_rule(collector_rule_id, domain_id)
        rule_type = collector_rule_vo.rule_type

        if rule_type == 'MANAGED':
            raise ERROR_NOT_ALLOWED_TO_DELETE_RULE()

        collector_id = collector_rule_vo.collector_id
        self.collector_rule_mgr.delete_collector_rule_by_vo(collector_rule_vo)

        collector_rule_vos = self._get_all_collector_rules(collector_id, rule_type, domain_id)

        i = 0
        for collector_rule_vo in collector_rule_vos:
            self.collector_rule_mgr.update_collector_rule_by_vo({'order': i + 1}, collector_rule_vo)
            i += 1

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['collector_rule_id', 'domain_id'])
    def get(self, params):
        """ Get collector rule

        Args:
            params (dict): {
                'collector_rule_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            collector_rule_vo (object)
        """
        collector_rule_id = params['collector_rule_id']
        domain_id = params['domain_id']

        return self.collector_rule_mgr.get_collector_rule(collector_rule_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['collector_rule_id', 'name', 'collector_id', 'domain_id'])
    @append_keyword_filter(['collector_rule_id', 'name'])
    def list(self, params):
        """ List collector rule

        Args:
            params (dict): {
                'collector_rule_id': 'str',
                'name': 'str',
                'collector_id': 'str',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            collector_rule_vos (object)
            total_count
        """
        query = params.get('query', {})
        return self.collector_rule_mgr.list_collector_rules(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['collector_rule_id', 'name'])
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
        return self.collector_rule_mgr.stat_collector_rules(query)

    def _check_actions(self, actions, domain_id):
        if 'change_project' in actions:
            project_id = actions['change_project']

            identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
            identity_mgr.get_project(project_id, domain_id)

        if 'match_project' in actions:
            if 'source' not in actions['match_project']:
                raise ERROR_REQUIRED_PARAMETER(key='actions.match_project.source')

        if 'match_service_account' in actions:
            if 'source' not in actions['match_service_account']:
                raise ERROR_REQUIRED_PARAMETER(key='actions.match_service_account.source')

    def _get_highest_order(self, collector_id, rule_type, domain_id):
        collector_rule_vos = self.collector_rule_mgr.filter_collector_rules(collector_id=collector_id,
                                                                            rule_type=rule_type,
                                                                            domain_id=domain_id)

        return collector_rule_vos.count()

    def _get_all_collector_rules(self, collector_id, rule_type, domain_id, exclude_collector_rule_id=None):
        query = {
            'filter': [
                {
                    'k': 'domain_id',
                    'v': domain_id,
                    'o': 'eq'
                },
                {
                    'k': 'collector_id',
                    'v': collector_id,
                    'o': 'eq'
                },
                {
                    'k': 'rule_type',
                    'v': rule_type,
                    'o': 'eq'
                },
            ],
            'sort': {
                'key': 'order'
            }
        }

        if exclude_collector_rule_id is not None:
            query['filter'].append({
                'k': 'collector_rule_id',
                'v': exclude_collector_rule_id,
                'o': 'not'
            })

        collector_rule_vos, total_count = self.collector_rule_mgr.list_collector_rules(query)
        return list(collector_rule_vos)

    @staticmethod
    def _check_conditions(conditions):
        for condition in conditions:
            key = condition.get('key')
            value = condition.get('value')
            operator = condition.get('operator')

            if not (key and value and operator):
                raise ERROR_INVALID_PARAMETER(key='conditions', reason='Condition should have key, value and operator.')

            if key not in _SUPPORTED_CONDITION_KEYS:
                if not (fnmatch.fnmatch(key, 'additional_info.*') or fnmatch.fnmatch(key, 'tags.*')):
                    raise ERROR_INVALID_PARAMETER(key='conditions.key',
                                                  reason=f'Unsupported key. '
                                                         f'({" | ".join(_SUPPORTED_CONDITION_KEYS)})')
            if operator not in _SUPPORTED_CONDITION_OPERATORS:
                raise ERROR_INVALID_PARAMETER(key='conditions.operator',
                                              reason=f'Unsupported operator. '
                                                     f'({" | ".join(_SUPPORTED_CONDITION_OPERATORS)})')

    @staticmethod
    def _check_order(order):
        if order <= 0:
            raise ERROR_INVALID_PARAMETER(key='order', reason='The order must be greater than 0.')
