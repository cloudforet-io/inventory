import os
import uuid
import random
import unittest
import pprint
from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestCloudServiceType(unittest.TestCase):
    config = utils.load_yaml_from_file(
        os.environ.get('SPACEONE_TEST_CONFIG_FILE', './config.yml'))

    pp = pprint.PrettyPrinter(indent=4)
    identity_v1 = None
    inventory_v1 = None
    domain = None
    domain_owner = None
    owner_id = None
    owner_pw = None
    token = None

    @classmethod
    def setUpClass(cls):
        super(TestCloudServiceType, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestCloudServiceType, cls).tearDownClass()
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
            'name': name
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
        self.cloud_service_type = None
        self.cloud_service_types = []

    def tearDown(self):
        for cloud_svc_type in self.cloud_service_types:
            self.inventory_v1.CloudServiceType.delete(
                {'cloud_service_type_id': cloud_svc_type.cloud_service_type_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

    def _print_data(self, message, description=None):
        print()
        if description:
            print(f'[ {description} ]')

        self.pp.pprint(MessageToDict(message, preserving_proto_field_name=True))

    def test_create_cloud_service_type(self, name=None, provider=None, group=None):
        """ Create Cloud Service Type
        """

        if name is None:
            name = random_string()

        if provider is None:
            provider = random_string()

        if group is None:
            group = random_string()

        params = {
            'name': name,
            'provider': provider,
            'group': group,
            'resource_type': 'inventory.Server',
            'is_primary': True,
            'is_major': True,
            # 'metadata': {
            #     'view': {
            #         'search': [{
            #             'name': 'Provider',
            #             'key': 'provider'
            #         }, {
            #             'name': 'Project',
            #             'key': 'project'
            #         }]
            #     }
            # },
            'domain_id': self.domain.domain_id
        }

        self.cloud_service_type = self.inventory_v1.CloudServiceType.create(
            params, metadata=(('token', self.token),)
        )

        self._print_data(self.cloud_service_type, 'test_create_cloud_service_type')

        self.cloud_service_types.append(self.cloud_service_type)
        self.assertEqual(self.cloud_service_type.name, name)

    def test_create_cloud_service_type_metadata(self, name=None, provider=None, group=None):
        """ Create Cloud Service Type with data source
        """

        if name is None:
            name = random_string()

        if provider is None:
            provider = random_string()

        if group is None:
            group = random_string()

        params = {
            'name': name,
            'provider': provider,
            'group': group,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service_type = self.inventory_v1.CloudServiceType.create(params, metadata=(('token', self.token),))
        self.cloud_service_types.append(self.cloud_service_type)
        self.assertEqual(self.cloud_service_type.name, name)

    def test_create_cloud_service_type_with_service_code(self, name=None, provider=None):
        """ Create Cloud Service Type with group
        """

        if name is None:
            name = random_string()

        if provider is None:
            provider = random_string()

        group = random_string()
        service_code = random_string()

        params = {
            'name': name,
            'provider': provider,
            'group': group,
            'service_code': service_code,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service_type = self.inventory_v1.CloudServiceType.create(params, metadata=(('token', self.token),))
        self.cloud_service_types.append(self.cloud_service_type)

        self._print_data(self.cloud_service_type, 'test_create_cloud_service_type_with_service_code')
        self.assertEqual(self.cloud_service_type.service_code, service_code)

    def test_create_cloud_service_type_labels(self, name=None, provider=None, group=None):
        """ Create Cloud Service Type with group
        """

        if name is None:
            name = random_string()

        if provider is None:
            provider = random_string()

        if group is None:
            group = random_string()

        labels = [random_string(), random_string(), random_string()]

        params = {
            'name': name,
            'provider': provider,
            'group': group,
            'labels': labels,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service_type = self.inventory_v1.CloudServiceType.create(params, metadata=(('token', self.token),))
        self.cloud_service_types.append(self.cloud_service_type)
        self.assertEqual(self.cloud_service_type.labels, labels)

    def test_create_duplicate_cloud_service_type(self):
        name = random_string()
        provider = random_string()
        group = random_string()

        self.test_create_cloud_service_type(name=name, provider=provider, group=group)

        with self.assertRaises(Exception):
            self.test_create_cloud_service_type(name=name, provider=provider, group=group)

    def test_update_cloud_service_type_metadata(self):
        self.test_create_cloud_service_type()

        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            "is_primary": False,
            "is_major": False,
            'labels': ['aa', 'bb'],
            'metadata': {
                'view': {
                    'search': [{
                        'name': 'Provider',
                        'key': 'provider'
                    }, {
                        'name': 'Project',
                        'key': 'project'
                    }]
                }
            },
            'domain_id': self.domain.domain_id,
        }
        self.cloud_service_type = self.inventory_v1.CloudServiceType.update(
            param,
            metadata=(('token', self.token),)
        )

        self._print_data(self.cloud_service_type, 'test_update_cloud_service_type_metadata')

    def test_update_cloud_service_type_service_code(self):
        self.test_create_cloud_service_type()

        service_code = random_string()

        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            "is_primary": False,
            "is_major": False,
            'service_code': service_code,
            'domain_id': self.domain.domain_id,
        }
        self.cloud_service_type = self.inventory_v1.CloudServiceType.update(
            param,
            metadata=(('token', self.token),)
        )

        self._print_data(self.cloud_service_type, 'test_update_cloud_service_type_service_code')
        self.assertEqual(self.cloud_service_type.service_code, service_code)

    def test_update_cloud_service_type_resource_type(self):
        self.test_create_cloud_service_type()

        resource_type = 'inventory.CloudService'

        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            'resource_type': resource_type,
            'domain_id': self.domain.domain_id,
        }
        self.cloud_service_type = self.inventory_v1.CloudServiceType.update(
            param,
            metadata=(('token', self.token),)
        )

        self._print_data(self.cloud_service_type, 'test_update_cloud_service_type_resource_type')
        self.assertEqual(self.cloud_service_type.resource_type, resource_type)

    def test_update_cloud_service_type_label(self):
        self.test_create_cloud_service_type()

        labels = [random_string(), random_string(), random_string()]

        param = { 'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
                  'labels': labels,
                  'domain_id': self.domain.domain_id,
                }

        self.cloud_service_type = self.inventory_v1.CloudServiceType.update(param, metadata=(('token', self.token),))

        for _label in self.cloud_service_type.labels:
            self.assertEqual(_label, labels[0])
            break

    def test_update_cloud_service_type_tags(self):
        self.test_create_cloud_service_type()

        tags = [
            {
                'key': random_string(),
                'value': random_string()
            }, {
                'key': random_string(),
                'value': random_string()
            }
        ]
        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            'tags': tags,
            'domain_id': self.domain.domain_id
        }
        self.cloud_service_type = self.inventory_v1.CloudServiceType.update(param, metadata=(('token', self.token),))
        cloud_service_type_data = MessageToDict(self.cloud_service_type)
        self.assertEqual(cloud_service_type_data['tags'], tags)

    def test_get_cloud_service_type(self):
        name = 'test-cst'
        self.test_create_cloud_service_type(name=name)

        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            'domain_id': self.domain.domain_id
        }
        self.cloud_service_type = self.inventory_v1.CloudServiceType.get(param, metadata=(('token', self.token),))
        self.assertEqual(self.cloud_service_type.name, name)

    def test_list_cloud_service_types(self):
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()

        param = {
            'cloud_service_type_id': self.cloud_service_type.cloud_service_type_id,
            'domain_id': self.domain.domain_id
        }

        cloud_service_types = self.inventory_v1.CloudServiceType.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, cloud_service_types.total_count)

    def test_list_cloud_service_types_name(self):
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()

        param = {
            'name': self.cloud_service_type.name,
            'domain_id': self.domain.domain_id
        }

        cloud_svc_types = self.inventory_v1.CloudServiceType.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, cloud_svc_types.total_count)

    def test_list_cloud_service_types_group(self):
        group = random_string()

        self.test_create_cloud_service_type(group=group)
        self.test_create_cloud_service_type(group=group)
        self.test_create_cloud_service_type(group=group)
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()

        param = {
            'group': group,
            'domain_id': self.domain.domain_id
        }

        cloud_svc_types = self.inventory_v1.CloudServiceType.list(param, metadata=(('token', self.token),))

        self.assertEqual(3, cloud_svc_types.total_count)

    def test_list_query(self):
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()
        self.test_create_cloud_service_type()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'cloud_service_type_id',
                        'v': list(map(lambda cloud_service_type: cloud_service_type.cloud_service_type_id, self.cloud_service_types)),
                        'o': 'in'
                    }
                ]
            }
        }

        cloud_service_types = self.inventory_v1.CloudServiceType.list(param, metadata=(('token', self.token),))

        print(cloud_service_types)
        self.assertEqual(len(self.cloud_service_types), cloud_service_types.total_count)

    def test_stat_cloud_service_type(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'cloud_service_type_id',
                            'name': 'Id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Count'
                        }]
                    }
                },
                'sort': {
                    'name': 'Id',
                    'desc': True
                }
            }
        }

        result = self.inventory_v1.CloudServiceType.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_cloud_service_type')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

