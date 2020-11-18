import os
import uuid
import random
import unittest
from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestRegion(unittest.TestCase):
    config = utils.load_yaml_from_file(
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
        self.regions = []
        self.region = None
        self.zones = []
        self.zone = None
        self.users = []
        self.user = None

    def tearDown(self):
        for region in self.regions:
            self.inventory_v1.Region.delete(
                {'region_id': region.region_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

    def test_create_region(self, name=None, region_code='ap-northeast-2', provider='aws'):
        """ Create Region
        """

        if not name:
            name = random_string()

        params = {
            'name': name,
            'region_code': region_code,
            'provider': provider,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(
            params,
            metadata=(('token', self.token),))

        self.regions.append(self.region)
        self.assertEqual(self.region.name, name)

    def test_update_region_name(self):
        self.test_create_region(region_code='korea', provider='aws')

        name = random_string()
        param = {
            'region_id': self.region.region_id,
            'name': name,
            'domain_id': self.domain.domain_id,
        }
        self.region = self.inventory_v1.Region.update(
            param,
            metadata=(('token', self.token),))
        self.assertEqual(self.region.name, name)

    def test_update_region_tags(self):
        self.test_create_region(region_code='korea', provider='datacenter')

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = {
            'region_id': self.region.region_id,
            'tags': tags,
            'domain_id': self.domain.domain_id,
        }
        self.region = self.inventory_v1.Region.update(
            param,
            metadata=(('token', self.token),))
        self.assertEqual(MessageToDict(self.region.tags), tags)

    def test_get_region(self):
        name = 'test-region'
        self.test_create_region(name, region_code='ap-east-1')

        param = {
            'region_id': self.region.region_id,
            'domain_id': self.domain.domain_id
        }
        self.region = self.inventory_v1.Region.get(
            param,
            metadata=(('token', self.token),)
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
            metadata=(('token', self.token),))

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
            metadata=(('token', self.token),))

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
            metadata=(('token', self.token),))

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
            metadata=(('token', self.token),))

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
            metadata=(('token', self.token),))
        self.assertEqual(len(self.regions), regions.total_count)

    def test_stat_region(self):
        self.test_list_query(provider='azure')

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
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
                },
                'sort': {
                    'name': 'Count',
                    'desc': True
                }
            }
        }

        result = self.inventory_v1.Region.stat(
            params,
            metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

