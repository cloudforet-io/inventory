import os
import uuid
import random
import unittest
from langcodes import Language
from spaceone.core import config
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestNetworkType(unittest.TestCase):
    config = config.load_config(
        os.environ.get('SPACEONE_TEST_CONFIG_FILE', './config.yml'))

    identity_v1 = None
    inventory_v1 = None
    domain = None
    domain_owner = None
    owner_id = None
    owner_pw = None
    token = None

    @classmethod
    def setUpClass(cls):
        super(TestNetworkType, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestNetworkType, cls).tearDownClass()
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
        self.network_types = []
        self.network_type = None

    def tearDown(self):
        for network_type in self.network_types:
            self.inventory_v1.NetworkType.delete(
                {'network_type_id': network_type.network_type_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

    def test_create_network_type(self, name=None):
        if not name:
            name = random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.network_type = self.inventory_v1.NetworkType.create(params,
                                                          metadata=(('token', self.token),)
                                                          )

        self.network_types.append(self.network_type)
        self.assertEqual(self.network_type.name, name)

    def test_update_network_type_name(self):
        self.test_create_network_type()

        name = random_string()
        param = { 'network_type_id': self.network_type.network_type_id,
                  'name': name,
                  'domain_id': self.domain.domain_id,
                }
        self.network_type = self.inventory_v1.NetworkType.update(param,
                                                                 metadata=(('token', self.token),)
                                                                 )
        self.assertEqual(self.network_type.name, name)

    def test_update_network_type_tags(self):
        self.test_create_network_type()

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = { 'network_type_id': self.network_type.network_type_id,
                  'tags': tags,
                  'domain_id': self.domain.domain_id,
                }
        self.network_type = self.inventory_v1.NetworkType.update(param,
                                                                 metadata=(('token', self.token),)
                                                                 )
        self.assertEqual(MessageToDict(self.network_type.tags), tags)

    def test_get_network_type(self):
        name = 'test-ntype'
        self.test_create_network_type(name)

        param = {
            'network_type_id': self.network_type.network_type_id,
            'domain_id': self.domain.domain_id
        }
        self.network_type = self.inventory_v1.NetworkType.get(param,
                                                              metadata=(('token', self.token),)
                                                              )
        self.assertEqual(self.network_type.name, name)

    def test_list_network_type_id(self):
        self.test_create_network_type()
        self.test_create_network_type()

        param = {
            'network_type_id': self.network_type.network_type_id,
            'domain_id': self.domain.domain_id
        }

        network_types = self.inventory_v1.NetworkType.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network_types.total_count)

    def test_list_name(self):
        self.test_create_network_type()
        self.test_create_network_type()

        param = {
            'name': self.network_type.name,
            'domain_id': self.domain.domain_id
        }

        network_types = self.inventory_v1.NetworkType.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, network_types.total_count)

    def test_list_query(self):
        self.test_create_network_type()
        self.test_create_network_type()
        self.test_create_network_type()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'network_type_id',
                        'v': list(map(lambda ntype: ntype.network_type_id, self.network_types)),
                        'o': 'in'
                    }
                ]
            }
        }

        ntypes = self.inventory_v1.NetworkType.list(param, metadata=(('token', self.token),))
        self.assertEqual(len(self.network_types), ntypes.total_count)

    def test_stat_network_type(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'network_type_id',
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

        result = self.inventory_v1.NetworkType.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

