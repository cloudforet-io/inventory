import os
import unittest
import pprint
import random
from google.protobuf.json_format import MessageToDict

from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner


class TestRegion(unittest.TestCase):
    config = utils.load_yaml_from_file(
        os.environ.get('SPACEONE_TEST_CONFIG_FILE', './config.yml'))

    pp = pprint.PrettyPrinter(indent=4)
    identity_v1 = None
    inventory_v1 = None
    owner_id = None
    owner_pw = None
    owner_token = None

    @classmethod
    def setUpClass(cls):
        super(TestRegion, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})

        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestRegion, cls).tearDownClass()
        cls.identity_v1.DomainOwner.delete(
            {
                'domain_id': cls.domain.domain_id,
                'owner_id': cls.owner_id
            }, metadata=(('token', cls.owner_token),)
        )

        cls.identity_v1.Domain.delete(
            {
                'domain_id': cls.domain.domain_id
            }, metadata=(('token', cls.owner_token),)
        )

    @classmethod
    def _create_domain(cls):
        name = utils.random_string()
        params = {
            'name': name
        }
        cls.domain = cls.identity_v1.Domain.create(params)

    @classmethod
    def _create_domain_owner(cls):
        cls.owner_id = utils.random_string() + '@mz.co.kr'
        cls.owner_pw = utils.generate_password()

        params = {
            'owner_id': cls.owner_id,
            'password': cls.owner_pw,
            'domain_id': cls.domain.domain_id
        }

        owner = cls.identity_v1.DomainOwner.create(
            params
        )
        cls.domain_owner = owner

    @classmethod
    def _issue_owner_token(cls):
        token_param = {
            'user_type': 'DOMAIN_OWNER',
            'user_id': cls.owner_id,
            'credentials': {
                'password': cls.owner_pw
            },
            'domain_id': cls.domain.domain_id
        }

        issue_token = cls.identity_v1.Token.issue(token_param)
        cls.owner_token = issue_token.access_token

    def setUp(self):
        self.regions = []
        self.region = None
        self.users = []
        self.user = None

    def tearDown(self):
        for region in self.regions:
            self.inventory_v1.Region.delete(
                {'region_id': region.region_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )

    def _print_data(self, message, description=None):
        print()
        if description:
            print(f'[ {description} ]')

        self.pp.pprint(MessageToDict(message, preserving_proto_field_name=True))

    def test_create_region(self, name=None, region_code='ap-northeast-2', provider='aws'):
        """ Create Region
        """

        if not name:
            name = utils.random_string()

        params = {
            'name': name,
            'region_code': region_code,
            'provider': provider,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(
            params,
            metadata=(('token', self.owner_token),))

        self.regions.append(self.region)
        self._print_data(self.region, 'test_create_region')
        self.assertEqual(self.region.name, name)

    def test_update_region_name(self):
        self.test_create_region(region_code='korea', provider='aws')

        name = utils.random_string()
        param = {
            'region_id': self.region.region_id,
            'name': name,
            'domain_id': self.domain.domain_id,
        }
        self.region = self.inventory_v1.Region.update(
            param,
            metadata=(('token', self.owner_token),))
        self._print_data(self.region, 'test_update_region_name')
        self.assertEqual(self.region.name, name)

    def test_update_region_tags(self):
        self.test_create_region(region_code='korea', provider='datacenter')

        tags = {
            utils.random_string(): utils.random_string(),
            utils.random_string(): utils.random_string()
        }
        param = {
            'region_id': self.region.region_id,
            'tags': tags,
            'domain_id': self.domain.domain_id
        }
        self.region = self.inventory_v1.Region.update(
            param,
            metadata=(('token', self.owner_token),))
        region_data = MessageToDict(self.region)
        self.assertEqual(region_data['tags'], tags)

    def test_get_region(self):
        name = 'test-region'
        self.test_create_region(name, region_code='ap-east-1')

        param = {
            'region_id': self.region.region_id,
            'domain_id': self.domain.domain_id
        }
        self.region = self.inventory_v1.Region.get(
            param,
            metadata=(('token', self.owner_token),)
        )
        self.assertEqual(self.region.name, name)

    def test_list_region_id(self):
        self.test_create_region(name='test-xxx', region_code='us-east-1', provider='aws')
        self.test_create_region(name='test-yyy', region_code='us-east-2', provider='aws')

        param = {
            'region_id': self.region.region_id,
            'domain_id': self.domain.domain_id
        }

        regions = self.inventory_v1.Region.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(1, regions.total_count)

    def test_list_region_code(self):
        self.test_create_region(name='test-xxx', region_code='us-west-1', provider='aws')
        self.test_create_region(name='test-yyy', region_code='us-west-2', provider='aws')

        param = {
            'region_code': self.region.region_code,
            'domain_id': self.domain.domain_id
        }

        regions = self.inventory_v1.Region.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(1, regions.total_count)

    def test_list_provider(self):
        self.test_create_region(name='test-xxx', region_code='eu-east-1', provider='aws')
        self.test_create_region(name='test-yyy', region_code='eu-east-2', provider='aws')

        param = {
            'provider': self.region.provider,
            'domain_id': self.domain.domain_id
        }

        regions = self.inventory_v1.Region.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(2, regions.total_count)

    def test_duplicate_region_code(self):
        self.test_create_region(name='test-xxx', region_code='eu-west-1', provider='aws')

        with self.assertRaises(Exception):
            self.test_create_region(
                name='test-yyy',
                region_code='eu-west-1',
                provider='aws'
            )

    def test_list_name(self):
        self.test_create_region(region_code='eu-north-1', provider='aws')
        self.test_create_region(region_code='eu-north-2', provider='aws')

        param = {
            'name': self.region.name,
            'domain_id': self.domain.domain_id
        }

        regions = self.inventory_v1.Region.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(1, regions.total_count)

    def test_list_query(self, provider='google_cloud'):
        self.test_create_region(region_code='ap-northeast-1', provider=provider)
        self.test_create_region(region_code='ap-northeast-2', provider=provider)
        self.test_create_region(region_code='ap-northeast-3', provider=provider)

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'region_id',
                        'v': list(map(lambda region: region.region_id, self.regions)),
                        'o': 'in'
                    }
                ]
            }
        }

        regions = self.inventory_v1.Region.list(
            param,
            metadata=(('token', self.owner_token),))
        self.assertEqual(len(self.regions), regions.total_count)

    def test_stat_region(self):
        self.test_list_query(provider='azure')

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': [{
                    'group': {
                        'keys': [{
                            'key': 'region_id',
                            'name': 'Id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Count'
                        }]
                    }
                }, {
                    'sort': {
                        'key': 'Count',
                        'desc': True
                    }
                }]
            }
        }

        result = self.inventory_v1.Region.stat(
            params,
            metadata=(('token', self.owner_token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

