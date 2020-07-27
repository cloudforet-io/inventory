import os
import uuid
import unittest
from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestNetworkPolicy(unittest.TestCase):
    config = utils.load_yaml_from_file(
        os.environ.get('SPACEONE_TEST_CONFIG_FILE', './config.yml'))

    identity_v1 = None
    inventory_v1 = None
    domain = None
    domain_owner = None
    owner_id = None
    owner_pw = None
    token = None
    region = None
    zone = None

    @classmethod
    def setUpClass(cls):
        super(TestNetworkPolicy, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestNetworkPolicy, cls).tearDownClass()
        cls.identity_v1.DomainOwner.delete({
            'domain_id': cls.domain.domain_id,
            'owner_id': cls.owner_id
        })

        if cls.domain:
            cls.identity_v1.Domain.delete({'domain_id': cls.domain.domain_id})

    @classmethod
    def _create_domain(cls):
        name = utils.random_string()
        param = {
            'name': name,
            'tags': {utils.random_string(): utils.random_string(), utils.random_string(): utils.random_string()},
            'config': {
                'aaa': 'bbbb'
            }
        }

        cls.domain = cls.identity_v1.Domain.create(param)
        print(f'domain_id: {cls.domain.domain_id}')
        print(f'domain_name: {cls.domain.name}')

    @classmethod
    def _create_domain_owner(cls):
        cls.owner_id = utils.random_string()[0:10]
        cls.owner_pw = 'qwerty'

        param = {
            'owner_id': cls.owner_id,
            'password': cls.owner_pw,
            'name': 'Steven' + utils.random_string()[0:5],
            'timezone': 'utc+9',
            'email': 'Steven' + utils.random_string()[0:5] + '@mz.co.kr',
            'mobile': '+821026671234',
            'domain_id': cls.domain.domain_id
        }

        owner = cls.identity_v1.DomainOwner.create(
            param
        )
        cls.domain_owner = owner
        print(f'owner_id: {cls.owner_id}')
        print(f'owner_pw: {cls.owner_pw}')

    @classmethod
    def _issue_owner_token(cls):
        token_param = {
            'credentials': {
                'user_type': 'DOMAIN_OWNER',
                'user_id': cls.owner_id,
                'password': cls.owner_pw
            },
            'domain_id': cls.domain.domain_id
        }

        issue_token = cls.identity_v1.Token.issue(token_param)
        cls.token = issue_token.access_token
        print(f'token: {cls.token}')

    def setUp(self):
        self.network_policies = []
        self.network_policy = None

        self._create_zone()

    def tearDown(self):
        for network_policy in self.network_policies:
            self.inventory_v1.NetworkPolicy.delete(
                {'network_policy_id': network_policy.network_policy_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        if self.zone:
            self.inventory_v1.Zone.delete({'zone_id': self.zone.zone_id,
                                           'domain_id': self.domain.domain_id},
                                           metadata=(('token', self.token),))

        if self.region:
            self.inventory_v1.Region.delete({'region_id': self.region.region_id,
                                             'domain_id': self.domain.domain_id},
                                             metadata=(('token', self.token),))

    def _create_region(self, name=None):
        if not name:
            name = random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(params,
                                                      metadata=(('token', self.token),)
                                                      )

    def _create_zone(self, name=None):
        self._create_region()

        if not name:
            name = random_string()

        params = {
            'name': name,
            'region_id': self.region.region_id,
            'domain_id': self.domain.domain_id
        }

        self.zone = self.inventory_v1.Zone.create(params,
                                                  metadata=(('token', self.token),)
                                                  )

    def test_create_network_policy(self, name=None, data=None):
        if not name:
            name = random_string()

        params = {
            'name': name,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        if data is not None:
            params.update({
                'data': data
            })

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        self.network_policies.append(self.network_policy)
        self.assertEqual(self.network_policy.name, name)

    def test_create_network_policy_with_routing_tables(self):
        name = random_string()

        routing_tables = [
            {
                'cidr': '172.16.0.0/16',
                'destination': '172.16.0.1'
            }
        ]

        params = {
            'name': name,
            'routing_tables': routing_tables,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        for rt in self.network_policy.routing_tables:
            routing_table = MessageToDict(rt)

        self.network_policies.append(self.network_policy)
        self.assertEqual(routing_table, routing_tables[0])

    def test_create_network_policy_with_dns(self):
        name = random_string()

        dns = [
            '172.16.0.1',
            '172.16.0.2',
            '172.16.0.3'
        ]

        params = {
            'name': name,
            'dns': dns,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        self.network_policies.append(self.network_policy)
        self.assertEqual(self.network_policy.dns, dns)

    def test_create_network_policy_data(self):
        data = {
            'vpc': {
                'rtable_id': 'rtable-xxxxxxx'
            }
        }

        self.test_create_network_policy(data=data)
        self.assertEqual(MessageToDict(self.network_policy.data), data)

    def test_update_network_policy_name(self):
        self.test_create_network_policy()

        name = random_string()
        param = { 'network_policy_id': self.network_policy.network_policy_id,
                  'name': name,
                  'domain_id': self.domain.domain_id,
                }
        self.network_policy = self.inventory_v1.NetworkPolicy.update(param,
                                                                     metadata=(('token', self.token),)
                                                                     )
        self.assertEqual(self.network_policy.name, name)

    def test_update_network_policy_data(self):
        data = {
            'vpc': {
                'rtable_id': 'rtable-xxxxxxx'
            }
        }

        self.test_create_network_policy()

        param = { 'network_policy_id': self.network_policy.network_policy_id,
                  'data': data,
                  'domain_id': self.domain.domain_id,
                }
        self.network_policy = self.inventory_v1.NetworkPolicy.update(param,
                                                                     metadata=(('token', self.token),)
                                                                    )
        self.assertEqual(MessageToDict(self.network_policy.data), data)

    def test_update_network_policy_tags(self):
        self.test_create_network_policy()

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = { 'network_policy_id': self.network_policy.network_policy_id,
                  'tags': tags,
                  'domain_id': self.domain.domain_id,
                }
        self.network_policy = self.inventory_v1.NetworkPolicy.update(param,
                                                                     metadata=(('token', self.token),)
                                                                    )
        self.assertEqual(MessageToDict(self.network_policy.tags), tags)

    def test_update_network_policy_with_routing_tables(self):
        name = random_string()

        routing_tables = [
            {
                'cidr': '172.16.0.0/16',
                'destination': '172.16.0.1'
            }
        ]

        params = {
            'name': name,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )
        self.network_policies.append(self.network_policy)

        params = {
            'network_policy_id': self.network_policy.network_policy_id,
            'routing_tables': routing_tables,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.update(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        for rt in self.network_policy.routing_tables:
            routing_table = MessageToDict(rt)

        self.assertEqual(routing_table, routing_tables[0])

    def test_udpate_network_policy_with_dns(self):
        name = random_string()

        dns = [
            '172.16.0.1',
            '172.16.0.2',
            '172.16.0.3'
        ]

        params = {
            'name': name,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )
        self.network_policies.append(self.network_policy)

        params = {
            'network_policy_id': self.network_policy.network_policy_id,
            'dns': dns,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.update(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        self.assertEqual(self.network_policy.dns, dns)

    def test_get_network_policy(self):
        name = 'test-npolicy'
        self.test_create_network_policy(name)

        param = {
            'network_policy_id': self.network_policy.network_policy_id,
            'domain_id': self.domain.domain_id
        }
        self.network_policy = self.inventory_v1.NetworkPolicy.get(param,
                                                                  metadata=(('token', self.token),)
                                                                  )
        self.assertEqual(self.network_policy.name, name)

    def test_list_network_policy_id(self):
        self.test_create_network_policy()
        self.test_create_network_policy()

        param = {
            'network_policy_id': self.network_policy.network_policy_id,
            'domain_id': self.domain.domain_id
        }

        network_policies = self.inventory_v1.NetworkPolicy.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network_policies.total_count)

    def test_list_name(self):
        self.test_create_network_policy()
        self.test_create_network_policy()

        param = {
            'name': self.network_policy.name,
            'domain_id': self.domain.domain_id
        }

        network_policies = self.inventory_v1.NetworkPolicy.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network_policies.total_count)

    def test_list_query(self):
        self.test_create_network_policy()
        self.test_create_network_policy()
        self.test_create_network_policy()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'network_policy_id',
                        'v': list(map(lambda npolicy: npolicy.network_policy_id, self.network_policies)),
                        'o': 'in'
                    }
                ]
            }
        }

        npolicies = self.inventory_v1.NetworkPolicy.list(param, metadata=(('token', self.token),))
        self.assertEqual(len(self.network_policies), npolicies.total_count)

    def test_list_query_with_dns(self):
        self.test_create_network_policy()
        self.test_create_network_policy()
        self.test_create_network_policy()

        dns = [
            '172.16.0.1',
            '172.16.0.2'
        ]

        params = {
            'network_policy_id': self.network_policy.network_policy_id,
            'dns': dns,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.update(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'dns',
                        'v': '172.16.0.2',
                        'o': 'eq'
                    }
                ]
            }
        }

        npolicies = self.inventory_v1.NetworkPolicy.list(param, metadata=(('token', self.token),))
        self.assertEqual(1, npolicies.total_count)

    def test_list_query_with_routing_tables(self):
        self.test_create_network_policy()
        self.test_create_network_policy()
        self.test_create_network_policy()

        routing_tables = [
            {
                'cidr': '172.16.0.0/16',
                'destination': '172.16.0.1'
            }
        ]

        params = {
            'network_policy_id': self.network_policy.network_policy_id,
            'routing_tables': routing_tables,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.update(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'routing_tables.cidr',
                        'v': '172.16.0.0/16',
                        'o': 'eq'
                    }
                ]
            }
        }

        npolicies = self.inventory_v1.NetworkPolicy.list(param, metadata=(('token', self.token),))
        self.assertEqual(1, npolicies.total_count)

    def test_stat_network_policy(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'network_policy_id',
                            'name': 'Id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Count'
                        }]
                    }
                },
                'sort': {
                    'name': 'Count',
                    'desc': True
                }
            }
        }

        result = self.inventory_v1.NetworkPolicy.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

