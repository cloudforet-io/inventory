import logging
import fnmatch

from spaceone.core.service import *
from spaceone.inventory.error import *
from spaceone.inventory.manager.collector_manager import CollectorManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.manager.identity_manager import IdentityManager
from spaceone.inventory.model.collector_rule_model import CollectorRule

_LOGGER = logging.getLogger(__name__)

_SUPPORTED_CONDITION_KEYS = [
    "provider",
    "cloud_service_group",
    "cloud_service_type",
    "region_code",
    "account",
    "reference.resource_id",
    "data.<key>",
    "tags.<key>",
]
_SUPPORTED_CONDITION_OPERATORS = ["eq", "contain", "not", "not_contain"]


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CollectorRuleService(BaseService):
    resource = "CollectorRule"

    def __init__(self, metadata):
        super().__init__(metadata)
        self.collector_rule_mgr: CollectorRuleManager = self.locator.get_manager(
            "CollectorRuleManager"
        )

    @transaction(
        permission="inventory:CollectorRule.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_id", "conditions_policy", "actions", "domain_id"])
    def create(self, params: dict) -> CollectorRule:
        """Create Collector rule

        Args:
            params (dict): {
                'collector_id': 'str',          # required
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'str',     # required
                'actions': 'dict',              # required
                'options': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            collector_rule_vo (object)
        """

        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        collector_id = params["collector_id"]
        conditions = params.get("conditions", [])
        conditions_policy = params["conditions_policy"]
        actions = params["actions"]

        collector_mgr: CollectorManager = self.locator.get_manager("CollectorManager")
        collector_vo = collector_mgr.get_collector(
            collector_id, domain_id, workspace_id
        )

        params["collector"] = collector_vo
        params["rule_type"] = "CUSTOM"
        params["resource_group"] = collector_vo.resource_group

        if conditions_policy == "ALWAYS":
            params["conditions"] = []
        else:
            if len(conditions) == 0:
                raise ERROR_REQUIRED_PARAMETER(key="conditions")
            else:
                self._check_conditions(conditions)

        self._check_actions(actions, domain_id)

        params["order"] = (
            self._get_highest_order(collector_id, params["rule_type"], domain_id) + 1
        )

        return self.collector_rule_mgr.create_collector_rule(params)

    @transaction(
        permission="inventory:CollectorRule.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_rule_id", "domain_id"])
    def update(self, params: dict) -> CollectorRule:
        """Update collector rule
        Args:
            params (dict): {
                'collector_rule_id': 'str',     # required
                'name': 'str',
                'conditions': 'list',
                'conditions_policy': 'list',
                'actions': 'dict',
                'options': 'dict'
                'tags': 'dict'
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            collector_rule_vo (object)
        """

        collector_rule_id = params["collector_rule_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        conditions_policy = params.get("conditions_policy")
        conditions = params.get("conditions", [])

        collector_rule_vo = self.collector_rule_mgr.get_collector_rule(
            collector_rule_id, domain_id, workspace_id
        )

        if collector_rule_vo.rule_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_TO_UPDATE_RULE()

        if conditions_policy:
            if conditions_policy == "ALWAYS":
                params["conditions"] = []
            else:
                if len(conditions) == 0:
                    raise ERROR_REQUIRED_PARAMETER(key="conditions")
                else:
                    self._check_conditions(conditions)

        if "actions" in params:
            self._check_actions(params["actions"], domain_id)

        return self.collector_rule_mgr.update_collector_rule_by_vo(
            params, collector_rule_vo
        )

    @transaction(
        permission="inventory:CollectorRule.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_rule_id", "order", "domain_id"])
    def change_order(self, params: dict) -> CollectorRule:
        """Change collector rule's order

        Args:
            params (dict): {
                'collector_rule_id': 'str',     # required
                'order': 'int',                 # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            collector_rule_vo (object)
        """

        collector_rule_id = params["collector_rule_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        order = params["order"]

        target_rule_vo: CollectorRule = self.collector_rule_mgr.get_collector_rule(
            collector_rule_id, domain_id, workspace_id
        )

        self._check_order(order)

        if target_rule_vo.rule_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_TO_CHANGE_ORDER()

        if target_rule_vo.order == order:
            return target_rule_vo

        highest_order = self._get_highest_order(
            target_rule_vo.collector_id,
            target_rule_vo.rule_type,
            target_rule_vo.domain_id,
        )

        if order > highest_order:
            raise ERROR_INVALID_PARAMETER(
                key="order",
                reason=f"There is no collector rules greater than the {str(order)} order.",
            )

        collector_rule_vos = self._get_all_collector_rules(
            target_rule_vo.collector_id,
            target_rule_vo.rule_type,
            target_rule_vo.domain_id,
            target_rule_vo.collector_rule_id,
        )

        collector_rule_vos.insert(order - 1, target_rule_vo)

        i = 0
        for collector_rule_vo in collector_rule_vos:
            if target_rule_vo != collector_rule_vo:
                self.collector_rule_mgr.update_collector_rule_by_vo(
                    {"order": i + 1}, collector_rule_vo
                )

            i += 1

        return self.collector_rule_mgr.update_collector_rule_by_vo(
            {"order": order}, target_rule_vo
        )

    @transaction(
        permission="inventory:CollectorRule.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER"],
    )
    @check_required(["collector_rule_id", "domain_id"])
    def delete(self, params):
        """Delete collector rule

        Args:
            params (dict): {
                'collector_rule_id': 'str',     # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        collector_rule_id = params["collector_rule_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")

        collector_rule_vo: CollectorRule = self.collector_rule_mgr.get_collector_rule(
            collector_rule_id, domain_id, workspace_id
        )

        rule_type = collector_rule_vo.rule_type
        if rule_type == "MANAGED":
            raise ERROR_NOT_ALLOWED_TO_DELETE_RULE()

        collector_id = collector_rule_vo.collector_id
        self.collector_rule_mgr.delete_collector_rule_by_vo(collector_rule_vo)

        collector_rule_vos = self._get_all_collector_rules(
            collector_id, rule_type, domain_id
        )

        i = 0
        for collector_rule_vo in collector_rule_vos:
            self.collector_rule_mgr.update_collector_rule_by_vo(
                {"order": i + 1}, collector_rule_vo
            )
            i += 1

    @transaction(
        permission="inventory:CollectorRule.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["collector_rule_id", "domain_id"])
    def get(self, params):
        """Get collector rule

        Args:
            params (dict): {
                'collector_rule_id': 'str',     # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            collector_rule_vo (object)
        """

        return self.collector_rule_mgr.get_collector_rule(
            params["collector_rule_id"], params["domain_id"], params.get("workspace_id")
        )

    @transaction(
        permission="inventory:CollectorRule.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(
        [
            "collector_rule_id",
            "name",
            "rule_type",
            "collector_id",
            "workspace_id",
            "domain_id",
        ]
    )
    @append_keyword_filter(["collector_rule_id", "name"])
    def list(self, params):
        """List collector rule

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)',
                'collector_rule_id': 'str',
                'name': 'str',
                'rule_type': 'str',
                'collector_id': 'str',
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            collector_rule_vos (object)
            total_count
        """

        query = params.get("query", {})
        return self.collector_rule_mgr.list_collector_rules(query)

    @transaction(
        permission="inventory:CollectorRule.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["workspace_id", "domain_id"])
    @append_keyword_filter(["collector_rule_id", "name"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)', # required
                'workspace_id': 'str',      # injected from auth
                'domain_id': 'str',         # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'
        """

        query = params.get("query", {})
        return self.collector_rule_mgr.stat_collector_rules(query)

    def _check_actions(self, actions: dict, domain_id: str) -> None:
        if "change_project" in actions:
            project_id = actions["change_project"]

            identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")
            identity_mgr.get_project(project_id, domain_id)

        if "match_project" in actions:
            if "source" not in actions["match_project"]:
                raise ERROR_REQUIRED_PARAMETER(key="actions.match_project.source")

        if "match_service_account" in actions:
            if "source" not in actions["match_service_account"]:
                raise ERROR_REQUIRED_PARAMETER(
                    key="actions.match_service_account.source"
                )

    def _get_highest_order(self, collector_id: str, rule_type: str, domain_id: str):
        collector_rule_vos = self.collector_rule_mgr.filter_collector_rules(
            collector_id=collector_id, rule_type=rule_type, domain_id=domain_id
        )

        return collector_rule_vos.count()

    def _get_all_collector_rules(
        self,
        collector_id: str,
        rule_type: str,
        domain_id: str,
        exclude_collector_rule_id: str = None,
    ):
        query = {
            "filter": [
                {"k": "domain_id", "v": domain_id, "o": "eq"},
                {"k": "collector_id", "v": collector_id, "o": "eq"},
                {"k": "rule_type", "v": rule_type, "o": "eq"},
            ],
            "sort": [{"key": "order"}],
        }

        if exclude_collector_rule_id is not None:
            query["filter"].append(
                {"k": "collector_rule_id", "v": exclude_collector_rule_id, "o": "not"}
            )

        collector_rule_vos, total_count = self.collector_rule_mgr.list_collector_rules(
            query
        )
        return list(collector_rule_vos)

    @staticmethod
    def _check_conditions(conditions: list) -> None:
        for condition in conditions:
            key = condition.get("key")
            value = condition.get("value")
            operator = condition.get("operator")

            if not (key and value and operator):
                raise ERROR_INVALID_PARAMETER(
                    key="conditions",
                    reason="Condition should have key, value and operator.",
                )

            if key not in _SUPPORTED_CONDITION_KEYS:
                if not (
                    fnmatch.fnmatch(key, "tags.*") or fnmatch.fnmatch(key, "data.*")
                ):
                    raise ERROR_INVALID_PARAMETER(
                        key="conditions.key",
                        reason=f"Unsupported key. "
                        f'({" | ".join(_SUPPORTED_CONDITION_KEYS)})',
                    )
            if operator not in _SUPPORTED_CONDITION_OPERATORS:
                raise ERROR_INVALID_PARAMETER(
                    key="conditions.operator",
                    reason=f"Unsupported operator. "
                    f'({" | ".join(_SUPPORTED_CONDITION_OPERATORS)})',
                )

    @staticmethod
    def _check_order(order: int) -> None:
        if order <= 0:
            raise ERROR_INVALID_PARAMETER(
                key="order", reason="The order must be greater than 0."
            )
