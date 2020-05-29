import os
import unittest
import uuid

from spaceone.core import config
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestNetwork(unittest.TestCase):
    config = config.load_config(
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
    api_key_obj = None

    @classmethod
    def setUpClass(cls):
        super(TestNetwork, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestNetwork, cls).tearDownClass()

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
        self.networks = []
        self.network = None

        self._create_zone()

    def tearDown(self):
        for network in self.networks:
            self.inventory_v1.Network.delete(
                {'network_id': network.network_id,
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

    def test_create_network(self, name=None, cidr=None, data=None):
        if not name:
            name = random_string()

        if not cidr:
            cidr = '172.16.0.0/16'

        params = {
            'name': name,
            'cidr': cidr,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        if data is not None:
            params.update({
                'data': data
            })

        self.network = self.inventory_v1.Network.create(params,
                                                        metadata=(('token', self.token),)
                                                        )

        self.networks.append(self.network)
        self.assertEqual(self.network.name, name)

    def test_create_network_data(self):
        data = {
            'vpc': {
                'vpc_id': 'vpc-xxxxxxx'
            }
        }

        self.test_create_network(data=data)
        self.assertEqual(MessageToDict(self.network.data), data)

    '''
    def test_create_network_duplicate_cidr(self):
        self.test_create_network()

        with self.assertRaises(Exception):
            self.test_create_network()

    def test_create_network_duplicate_cidr_2(self):
        self.test_create_network(cidr='172.16.0.0/16')

        with self.assertRaises(Exception):
            self.test_create_network(cidr='172.16.1.0/24')

    def test_create_network_duplicate_cidr_3(self):
        self.test_create_network(cidr='172.16.1.0/24')

        with self.assertRaises(Exception):
            self.test_create_network(cidr='172.16.0.0/16')
    '''

    def test_create_network_invalid_cidr_1(self):
        with self.assertRaises(Exception):
            self.test_create_network(cidr='172.16.0.0/48')

    def test_create_network_invalid_cidr_2(self):
        self.test_create_network(cidr='172.16.1.1')

        param = {
            'cidr': '172.16.1.1/32',
            'domain_id': self.domain.domain_id
        }

        network = self.inventory_v1.Network.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network.total_count)

    def test_update_network_name(self):
        self.test_create_network()

        name = random_string()
        param = { 'network_id': self.network.network_id,
                  'name': name,
                  'domain_id': self.domain.domain_id,
                }

        self.network = self.inventory_v1.Network.update(param,
                                                        metadata=(('token', self.token),)
                                                        )
        self.assertEqual(self.network.name, name)

    def test_update_network_tags(self):
        self.test_create_network()

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = { 'network_id': self.network.network_id,
                  'tags': tags,
                  'domain_id': self.domain.domain_id,
                }
        self.network = self.inventory_v1.Network.update(param,
                                                        metadata=(('token', self.token),)
                                                        )
        self.assertEqual(MessageToDict(self.network.tags), tags)

    def test_update_network_data(self):
        data = {
            'vpc': {
                'vpc_id': 'vpc-xxxxxxx'
            }
        }

        self.test_create_network()

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = { 'network_id': self.network.network_id,
                  'tags': tags,
                  'data': data,
                  'domain_id': self.domain.domain_id,
                }
        self.network = self.inventory_v1.Network.update(param,
                                                        metadata=(('token', self.token),)
                                                        )
        self.assertEqual(MessageToDict(self.network.data), data)

    def test_get_network(self):
        name = 'test-network'
        self.test_create_network(name)

        param = {
            'network_id': self.network.network_id,
            'domain_id': self.domain.domain_id
        }
        self.network = self.inventory_v1.Network.get(param,
                                                     metadata=(('token', self.token),)
                                                     )

        self.assertEqual(self.network.name, name)

    def test_list_network_id(self):
        self.test_create_network(cidr='172.16.1.0/24')
        self.test_create_network(cidr='172.16.2.0/24')

        param = {
            'network_id': self.network.network_id,
            'domain_id': self.domain.domain_id
        }

        network = self.inventory_v1.Network.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network.total_count)

    def test_list_name(self):
        self.test_create_network(cidr='172.16.1.0/24')
        self.test_create_network(cidr='172.16.2.0/24')

        param = {
            'name': self.network.name,
            'domain_id': self.domain.domain_id
        }

        network = self.inventory_v1.Network.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network.total_count)

    def test_list_query(self):
        self.test_create_network(cidr='172.16.1.0/24')
        self.test_create_network(cidr='172.16.2.0/24')
        self.test_create_network(cidr='172.16.3.0/24')

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'network_id',
                        'v': list(map(lambda network: network.network_id, self.networks)),
                        'o': 'in'
                    }
                ]
            }
        }

        networks = self.inventory_v1.Network.list(param, metadata=(('token', self.token),))
        self.assertEqual(len(self.networks), networks.total_count)

    def test_stat_network(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'cloud_service_id',
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

        result = self.inventory_v1.Network.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

