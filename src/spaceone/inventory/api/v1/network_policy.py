from spaceone.api.inventory.v1 import network_policy_pb2, network_policy_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class NetworkPolicy(BaseAPI, network_policy_pb2_grpc.NetworkPolicyServicer):

    pb2 = network_policy_pb2
    pb2_grpc = network_policy_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            return self.locator.get_info('NetworkPolicyInfo', npolicy_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            return self.locator.get_info('NetworkPolicyInfo', npolicy_service.update(params))

    def pin_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            return self.locator.get_info('NetworkPolicyInfo', npolicy_service.pin_data(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            npolicy_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            return self.locator.get_info('NetworkPolicyInfo', npolicy_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            npolicy_vos, total_count = npolicy_service.list(params)
            return self.locator.get_info('NetworkPoliciesInfo', npolicy_vos, total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NetworkPolicyService', metadata) as npolicy_service:
            return self.locator.get_info('StatisticsInfo', npolicy_service.stat(params))
