from spaceone.api.inventory.v1 import collector_rule_pb2, collector_rule_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class CollectorRule(BaseAPI, collector_rule_pb2_grpc.CollectorRuleServicer):

    pb2 = collector_rule_pb2
    pb2_grpc = collector_rule_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            return self.locator.get_info('CollectorRuleInfo', collector_rule_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            return self.locator.get_info('CollectorRuleInfo', collector_rule_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            collector_rule_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            return self.locator.get_info('CollectorRuleInfo', collector_rule_service.get(params))

    def change_order(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            return self.locator.get_info('CollectorRuleInfo', collector_rule_service.change_order(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            collector_rule_vos, total_count = collector_rule_service.list(params)
            return self.locator.get_info('CollectorRulesInfo', collector_rule_vos, total_count, minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CollectorRuleService', metadata) as collector_rule_service:
            return self.locator.get_info('StatisticsInfo', collector_rule_service.stat(params))
