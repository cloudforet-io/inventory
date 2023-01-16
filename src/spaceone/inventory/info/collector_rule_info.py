import functools
from typing import List
from spaceone.api.inventory.v1 import collector_rule_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

from spaceone.inventory.model.collector_rule_model import CollectorRule, CollectorRuleCondition, CollectorRuleOptions

__all__ = ['CollectorRuleInfo', 'CollectorRulesInfo']


def CollectorRuleConditionsInfo(condition_vos: List[CollectorRuleCondition]):
    if condition_vos is None:
        condition_vos = []

    conditions_info = []

    for vo in condition_vos:
        info = {
            'key': vo.key,
            'value': vo.value,
            'operator': vo.operator
        }

        conditions_info.append(collector_rule_pb2.CollectorRuleCondition(**info))

    return conditions_info


def CollectorRuleActionMatchRuleInfo(match_rule_data):
    if match_rule_data is None:
        return None

    info = {
        'source': match_rule_data.get('source'),
        'target': match_rule_data.get('target')
    }

    return collector_rule_pb2.MatchRule(**info)


def CollectorRuleActionsInfo(actions_data):
    if actions_data is None:
        return None
    else:
        info = {}

        for key, value in actions_data.items():
            if key in ['match_project', 'match_service_account']:
                info[key] = CollectorRuleActionMatchRuleInfo(value)
            elif key == 'add_additional_info':
                info[key] = change_struct_type(value)
            else:
                info[key] = value

        return collector_rule_pb2.CollectorRuleActions(**info)


def CollectorRuleOptionsInfo(vo: CollectorRuleOptions):
    if vo is None:
        return None
    else:
        info = {
            'stop_processing': vo.stop_processing
        }

        return collector_rule_pb2.CollectorRuleOptions(**info)


def CollectorRuleInfo(collector_rule_vo: CollectorRule, minimal=False):
    info = {
        'collector_rule_id': collector_rule_vo.collector_rule_id,
        'name': collector_rule_vo.name,
        'order': collector_rule_vo.order,
        'rule_type': collector_rule_vo.rule_type,
        'collector_id': collector_rule_vo.collector_id,
    }

    if not minimal:
        info.update({
            'conditions': CollectorRuleConditionsInfo(collector_rule_vo.conditions),
            'conditions_policy': collector_rule_vo.conditions_policy,
            'actions': CollectorRuleActionsInfo(collector_rule_vo.actions),
            'options': CollectorRuleOptionsInfo(collector_rule_vo.options),
            'tags': change_struct_type(collector_rule_vo.tags),
            'domain_id': collector_rule_vo.domain_id,
            'created_at': utils.datetime_to_iso8601(collector_rule_vo.created_at)
        })

    return collector_rule_pb2.CollectorRuleInfo(**info)


def CollectorRulesInfo(collector_rule_vos, total_count, **kwargs):
    return collector_rule_pb2.CollectorRulesInfo(results=list(
        map(functools.partial(CollectorRuleInfo, **kwargs), collector_rule_vos)), total_count=total_count)
