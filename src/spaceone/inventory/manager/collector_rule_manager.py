import logging
import functools
from spaceone.core import utils
from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.model.collector_rule_model import CollectorRule, CollectorRuleCondition

_LOGGER = logging.getLogger(__name__)


class CollectorRuleManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collector_rule_model: CollectorRule = self.locator.get_model('CollectorRule')
        self.identity_mgr: IdentityManager = self.locator.get_manager('IdentityManager')
        self._project_info = {}
        self._service_account_info = {}
        self._collector_rule_info = {}

    def create_collector_rule(self, params):
        def _rollback(collector_rule_vo: CollectorRule):
            _LOGGER.info(f'[create_collector_rule._rollback] Delete event rule : {collector_rule_vo.name} '
                         f'({collector_rule_vo.collector_rule_id})')
            collector_rule_vo.delete()

        collector_rule_vo: CollectorRule = self.collector_rule_model.create(params)
        self.transaction.add_rollback(_rollback, collector_rule_vo)

        return collector_rule_vo

    def update_collector_rule(self, params):
        collector_rule_vo: CollectorRule = self.get_collector_rule(params['collector_rule_id'], params['domain_id'])
        return self.update_collector_rule_by_vo(params, collector_rule_vo)

    def update_collector_rule_by_vo(self, params, collector_rule_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_collector_rule_by_vo._rollback] Revert Data : '
                         f'{old_data["collector_rule_id"]}')
            collector_rule_vo.update(old_data)

        self.transaction.add_rollback(_rollback, collector_rule_vo.to_dict())

        return collector_rule_vo.update(params)

    def delete_collector_rule(self, collector_rule_id, domain_id):
        collector_rule_vo: CollectorRule = self.get_collector_rule(collector_rule_id, domain_id)
        self.delete_collector_rule_by_vo(collector_rule_vo)

    def get_collector_rule(self, collector_rule_id, domain_id, only=None):
        return self.collector_rule_model.get(collector_rule_id=collector_rule_id, domain_id=domain_id, only=only)

    def filter_collector_rules(self, **conditions):
        return self.collector_rule_model.filter(**conditions)

    def list_collector_rules(self, query={}):
        return self.collector_rule_model.query(**query)

    def stat_collector_rules(self, query):
        return self.collector_rule_model.stat(**query)

    def change_cloud_service_data(self, collector_id, domain_id, cloud_service_data):
        managed_collector_rule_vos, custom_collector_rule_vos = self._get_collector_rules(collector_id, domain_id)

        cloud_service_data = self._apply_collector_rule_to_cloud_service_data(cloud_service_data,
                                                                              managed_collector_rule_vos,
                                                                              domain_id)

        cloud_service_data = self._apply_collector_rule_to_cloud_service_data(cloud_service_data,
                                                                              custom_collector_rule_vos,
                                                                              domain_id)

        return cloud_service_data

    def _apply_collector_rule_to_cloud_service_data(self, cloud_service_data, collector_rule_vos, domain_id):
        for collector_rule_vo in collector_rule_vos:
            is_match = self._change_cloud_service_data_by_rule(cloud_service_data, collector_rule_vo)

            if is_match:
                cloud_service_data = self._change_cloud_service_data_with_actions(cloud_service_data,
                                                                                  collector_rule_vo.actions,
                                                                                  domain_id)

            if is_match and collector_rule_vo.options.stop_processing:
                break

        return cloud_service_data

    def _change_cloud_service_data_with_actions(self, cloud_service_data, actions, domain_id):
        for action, value in actions.items():
            if action == 'change_project':
                cloud_service_data['project_id'] = value

            elif action == 'match_project':
                source = value['source']
                target_key = value.get('target', 'project_id')
                target_value = utils.get_dict_value(cloud_service_data, source)

                if target_value:
                    project_info = self._get_project(target_key, target_value, domain_id)

                    if project_info:
                        cloud_service_data['project_id'] = project_info['project_id']

            elif action == 'match_service_account':
                source = value['source']
                target_key = value.get('target', 'service_account_id')
                target_value = utils.get_dict_value(cloud_service_data, source)
                if target_value:
                    service_account_info = self._get_service_account(target_key, target_value, domain_id)
                    if service_account_info:
                        cloud_service_data['service_account_id'] = service_account_info['service_account_id']
                        cloud_service_data['project_id'] = service_account_info.get('project_info', {}).get('project_id')

        return cloud_service_data

    def _get_service_account(self, target_key, target_value, domain_id):
        if f'service-account:{domain_id}:{target_key}:{target_value}' in self._service_account_info:
            return self._service_account_info[f'service-account:{domain_id}:{target_key}:{target_value}']

        query = {
            'filter': [
                {'k': target_key, 'v': target_value, 'o': 'eq'},
                {'k': 'service_account_type', 'v': 'GENERAL', 'o': 'eq'}
            ],
            'only': ['service_account_id', 'project_info']
        }

        response = self.identity_mgr.list_service_accounts(query, domain_id)
        results = response.get('results', [])
        total_count = response.get('total_count', 0)

        service_account_info = None
        if total_count > 0:
            service_account_info = results[0]

        self._service_account_info[f'service-account:{domain_id}:{target_key}:{target_value}'] = service_account_info
        return service_account_info

    def _get_project(self, target_key, target_value, domain_id):
        if f'project:{domain_id}:{target_key}:{target_value}' in self._project_info:
            _LOGGER.debug(f'[_get_project] Hit the cache in self._project_info : {target_value}')
            return self._project_info[f'project:{domain_id}:{target_key}:{target_value}']

        query = {
            'filter': [
                {'k': target_key, 'v': target_value, 'o': 'eq'}
            ],
            'only': ['project_id']
        }

        query_hash = utils.dict_to_hash(query)
        response = self.identity_mgr.list_projects_with_cache(query, query_hash, domain_id)
        results = response.get('results', [])
        total_count = response.get('total_count', 0)

        project_info = None
        if total_count > 0:
            project_info = results[0]

        self._project_info[f'project:{domain_id}:{target_key}:{target_value}'] = project_info
        return project_info

    def _change_cloud_service_data_by_rule(self, cloud_service_data, collector_rule_vo: CollectorRule):
        conditions_policy = collector_rule_vo.conditions_policy

        if conditions_policy == 'ALWAYS':
            return True
        else:
            results = list(map(functools.partial(self._check_condition, cloud_service_data), collector_rule_vo.conditions))

            if conditions_policy == 'ALL':
                return all(results)
            else:
                return any(results)

    @staticmethod
    def delete_collector_rule_by_vo(collector_rule_vo):
        collector_rule_vo.delete()

    @staticmethod
    def _check_condition(cloud_service_data, condition: CollectorRuleCondition):
        cloud_service_value = utils.get_dict_value(cloud_service_data, condition.key)
        condition_value = condition.value
        operator = condition.operator

        if cloud_service_value is None:
            return False

        if operator == 'eq':
            if cloud_service_value == condition_value:
                return True
            else:
                return False
        elif operator == 'contain':
            if cloud_service_value.lower().find(condition_value.lower()) >= 0:
                return True
            else:
                return False
        elif operator == 'not':
            if cloud_service_value != condition_value:
                return True
            else:
                return False
        elif operator == 'not_contain':
            if cloud_service_value.lower().find(condition_value.lower()) < 0:
                return True
            else:
                return False

        return False

    def _get_collector_rules(self, collector_id, domain_id):
        if collector_id in self._collector_rule_info:
            return self._collector_rule_info[collector_id].get('managed', []), \
                   self._collector_rule_info[collector_id].get('custom', [])

        managed_query = self._make_collector_rule_query(collector_id, 'MANAGED', domain_id)
        managed_collector_rule_vos, total_count = self.list_collector_rules(managed_query)

        custom_query = self._make_collector_rule_query(collector_id, 'CUSTOM', domain_id)
        custom_collector_rule_vos, total_count = self.list_collector_rules(custom_query)

        self._collector_rule_info[collector_id] = {}
        self._collector_rule_info[collector_id]['managed'] = managed_collector_rule_vos
        self._collector_rule_info[collector_id]['custom'] = custom_collector_rule_vos

        return managed_collector_rule_vos, custom_collector_rule_vos

    @staticmethod
    def _make_collector_rule_query(collector_id, rule_type, domain_id):
        return {
            'filter': [
                {'k': 'collector_id', 'v': collector_id, 'o': 'eq'},
                {'k': 'domain_id', 'v': domain_id, 'o': 'eq'},
                {'k': 'rule_type', 'v': rule_type, 'o': 'eq'}
            ],
            'sort': {'key': 'order'}
        }
