import os
import unittest
import pprint
from google.protobuf.json_format import MessageToDict

from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner


class TestCloudService(unittest.TestCase):
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
        super(TestCloudService, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})

        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1', ssl_enabled=True)
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestCloudService, cls).tearDownClass()
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
        self.region = None
        self.project_group = None
        self.project = None
        self.cloud_service = None
        self.cloud_services = []

    def tearDown(self):
        for cloud_svc in self.cloud_services:
            self.inventory_v1.CloudService.delete(
                {'cloud_service_id': cloud_svc.cloud_service_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )

        if self.region is not None:
            self.inventory_v1.Region.delete(
                {'region_id': self.region.region_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )

        if self.project is not None:
            self.identity_v1.Project.delete(
                {'project_id': self.project.project_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )

        if self.project_group is not None:
            self.identity_v1.ProjectGroup.delete(
                {'project_group_id': self.project_group.project_group_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )

    def _create_region(self, name=None, provider='aws', region_code=None):
        """ Create Region
        """

        if not name:
            name = utils.random_string()

        if region_code is None:
            region_code = 'region-' + utils.random_string()

        params = {
            'name': name,
            'region_code': region_code,
            'provider': provider,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(params, metadata=(('token', self.owner_token),))

    def _create_project_group(self, name=None):
        """ Create Project Group
        """

        if not name:
            name = utils.random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.project_group = self.identity_v1.ProjectGroup.create(params, metadata=(('token', self.owner_token),))

    def _create_project(self, name=None, project_group=None):
        """ Create Project
        """

        if not name:
            name = utils.random_string()

        if not project_group:
            self._create_project_group()
            project_group = self.project_group

        params = {
            'name': name,
            'project_group_id': project_group.project_group_id,
            'domain_id': self.domain.domain_id
        }

        self.project = self.identity_v1.Project.create(params, metadata=(('token', self.owner_token),))

    def _print_data(self, message, description=None):
        print()
        if description:
            print(f'[ {description} ]')

        self.pp.pprint(MessageToDict(message, preserving_proto_field_name=True))

    def test_create_cloud_service(self, name=None, cloud_service_type=None, provider=None, data=None, group=None,
                                  metadata=None, **kwargs):
        """ Create Cloud Service
        """

        if name is None:
            name = utils.random_string()

        if cloud_service_type is None:
            cloud_service_type = utils.random_string()

        if provider is None:
            provider = utils.random_string()

        if group is None:
            group = utils.random_string()

        if data is None:
            data = {
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): {
                    utils.random_string(): utils.random_string(),
                    utils.random_string(): utils.random_string(),
                    utils.random_string(): utils.random_string(),
                    utils.random_string(): utils.random_string(),
                    utils.random_string(): utils.random_string(),
                    utils.random_string(): {
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                    },
                    utils.random_string(): [
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string(),
                        utils.random_string()
                    ],
                    utils.random_string(): {
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                        utils.random_string(): utils.random_string(),
                    },
                }
            }

        if metadata is None:
            metadata = {
                'view': {
                    'sub_data': {
                        "layouts": [{
                            "name": "AWS EC2",
                            "type": "item",
                            "options": {
                                "fields": [{
                                    "name": "Cloud Service ID",
                                    "key": "cloud_service_id"
                                }]
                            }
                        }]
                    }
                }
            }

        params = {
            'name': name,
            'cloud_service_type': cloud_service_type,
            'provider': provider,
            'cloud_service_group': group,
            'domain_id': self.domain.domain_id,
            'data': data,
            'metadata': metadata,
            "region_code": "ap-northeast-2",
            "reference": {
                "resource_id": "resource-xxxx",
                "external_link": "https://aaa.bbb.ccc/"
            },
        }

        metadata = (('token', self.owner_token),)
        ext_meta = kwargs.get('meta')

        if ext_meta:
            metadata += ext_meta

        self.cloud_service = self.inventory_v1.CloudService.create(params, metadata=metadata)
        self._print_data(self.cloud_service, 'test_create_cloud_service')

        self.cloud_services.append(self.cloud_service)
        self.assertEqual(self.cloud_service.provider, provider)

    def test_create_cloud_service_by_collector(self, name=None):
        """ Create Server by Collector
        """
        self._create_project_group()
        self._create_project(project_group=self.project_group)

        self.test_create_cloud_service(name, meta=(
            ('job_id', utils.generate_id('job')),
            ('job_task_id', utils.generate_id('job-task')),
            ('collector_id', utils.generate_id('collector')),
            ('plugin_id', utils.generate_id('plugin')),
            ('secret.secret_id', utils.generate_id('secret')),
            ('secret.service_account_id', utils.generate_id('sa')),
            ('secret.project_id', self.project.project_id),
            ('secret.provider', 'aws')
        ))

    def test_create_cloud_service_group(self, cloud_service_type=None, provider=None):
        """ Create Cloud Service with cloud service group
        """

        if cloud_service_type is None:
            cloud_service_type = utils.random_string()

        if provider is None:
            provider = utils.random_string()

        group = utils.random_string()

        params = {
            'provider': provider,
            'cloud_service_type': cloud_service_type,
            'cloud_service_group': group,
            'data': {
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.create(params, metadata=(('token', self.owner_token),))
        self.cloud_services.append(self.cloud_service)
        self.assertEqual(self.cloud_service.cloud_service_group, group)

    def test_create_cloud_service_region_code(self, cloud_service_type=None, cloud_service_group=None, provider=None):
        """ Create Cloud Service with region code
        """

        if cloud_service_type is None:
            cloud_service_type = utils.random_string()

        if cloud_service_group is None:
            cloud_service_group = utils.random_string()

        if provider is None:
            provider = utils.random_string()

        self._create_region()

        params = {
            'provider': provider,
            'cloud_service_type': cloud_service_type,
            'cloud_service_group': cloud_service_group,
            'data': {
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string(),
                utils.random_string(): utils.random_string()
            },
            'region_code': self.region.region_code,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.create(params, metadata=(('token', self.owner_token),))

        self._print_data(self.cloud_service, 'test_create_cloud_service_region_code')

        self.cloud_services.append(self.cloud_service)
        self.assertEqual(self.cloud_service.region_code, self.region.region_code)

    def test_update_cloud_service_name(self):
        self.test_create_cloud_service()

        name = utils.random_string()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'name': name,
            'data': self.cloud_service.data,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_name')
        self.assertEqual(self.cloud_service.name, name)

    def test_update_cloud_service_project_id(self):
        self._create_project()
        self.test_create_cloud_service()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_project_id_1')
        self.assertEqual(self.cloud_service.project_id, self.project.project_id)

        self._create_project()
        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_project_id_2')
        self.assertEqual(self.cloud_service.project_id, self.project.project_id)

    def test_update_cloud_service_release_project(self):
        self._create_project()
        self.test_create_cloud_service()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_release_project_1')

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'release_project': True,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_release_project_2')

        self.assertEqual(self.cloud_service.project_id, '')

    def test_update_cloud_service_region(self):
        self._create_region()
        self.test_create_cloud_service()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'region_code': self.region.region_code,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))

        self._print_data(self.cloud_service, 'test_update_cloud_service_region_code')
        self.assertEqual(self.cloud_service.region_code, self.region.region_code)

    def test_update_cloud_service_release_region(self):
        self._create_region()
        self.test_create_cloud_service()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'region_code': self.region.region_code,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_region_code')

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'release_region': True,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        self._print_data(self.cloud_service, 'test_update_cloud_service_release_region')

        self.assertEqual(self.cloud_service.region_code, '')

    def test_update_cloud_service_data(self, **kwargs):
        old_data = {
            'a': 'b',
            'c': 'd',
            'x': 'y',
            'z': {
                'y': {
                    'a': 1,
                    'b': 2
                },
                't': [
                    {
                        'a': 1,
                        'b': 2,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 3
                        }
                    },
                    {
                        'a': 1,
                        'b': 1,
                        'c': {
                            'x': 1,
                            'z': 3,
                            'y': 2
                        }
                    },
                    {
                        'a': 2,
                        'b': 3,
                        'c': {
                            'z': 3,
                            'y': 2,
                            'x': 1
                        }
                    }
                ]
            }
        }

        old_metadata = {
            'view': {
                'sub_data': {
                    "layouts": [{
                        "name": "AWS EC2",
                        "type": "item",
                        "options": {
                            "fields": [{
                                "name": "Cloud Service ID",
                                "key": "cloud_service_id"
                            }]
                        }
                    }]
                }
            }
        }

        self.test_create_cloud_service(data=old_data, metadata=old_metadata, **kwargs)

        data = {
            'a': 'xxx',
            'c': 8,
            'e': 'f',
            'z': {
                'y': {
                    'c': 3,
                    'a': 1,
                    'b': 2
                },
                't': [
                    {
                        'a': 2,
                        'b': 3,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 3
                        }
                    },
                    {
                        'a': 1,
                        'b': 2,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 3
                        }
                    },
                    {
                        'a': 1,
                        'b': 1,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 5
                        }
                    }
                ]
            }
        }

        metadata = {
            'view': {
                'sub_data': {
                    "layouts": [{
                        "name": "AWS EC2",
                        "type": "item",
                        "options": {
                            "fields": [{
                                "name": "New Cloud Service ID",
                                "key": "cloud_service_id"
                            }]
                        }
                    }]
                }
            },
            'change_history': {
                'exclude': [
                    'reference.external_link',
                    'data.e'
                ]
            }
        }

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'data': data,
            'metadata': metadata,
            'reference': {
                'resource_id': 'resource-yyyy',
                'external_link': 'https://ddd.eee.fff/'
            },
            'domain_id': self.domain.domain_id
        }

        metadata = (('token', self.owner_token),)
        ext_meta = kwargs.get('meta')

        if ext_meta:
            metadata += ext_meta

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=metadata)

        self._print_data(self.cloud_service, 'test_update_cloud_service_data')

        result_data = {
            'a': 'xxx',
            'c': 8,
            'x': 'y',
            'e': 'f',
            'z': {
                'y': {
                    'c': 3,
                    'a': 1,
                    'b': 2
                },
                't': [
                    {
                        'a': 2,
                        'b': 3,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 3
                        }
                    },
                    {
                        'a': 1,
                        'b': 2,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 3
                        }
                    },
                    {
                        'a': 1,
                        'b': 1,
                        'c': {
                            'y': 2,
                            'x': 1,
                            'z': 5
                        }
                    }
                ]
            }
        }

        self.assertEqual(MessageToDict(self.cloud_service.data), result_data)

    def test_update_cloud_service_data_by_collector(self):
        self.test_update_cloud_service_data(meta=(
            ('job_id', utils.generate_id('job')),
            ('job_task_id', utils.generate_id('job-task')),
            ('collector_id', utils.generate_id('collector')),
            ('plugin_id', utils.generate_id('plugin')),
            ('secret.secret_id', utils.generate_id('secret')),
            ('secret.service_account_id', utils.generate_id('sa')),
            ('secret.provider', 'aws')
        ))

    def test_update_cloud_service_tags(self):
        self.test_create_cloud_service()

        tags = {
            utils.random_string(): utils.random_string(),
            utils.random_string(): utils.random_string()
        }
        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'tags': tags,
            'domain_id': self.domain.domain_id
        }

        self.cloud_service = self.inventory_v1.CloudService.update(param, metadata=(('token', self.owner_token),))
        cloud_service_data = MessageToDict(self.cloud_service)
        self.assertEqual(cloud_service_data['tags'], tags)

    def test_get_cloud_service(self):
        cloud_service_type = 's3'
        provider = 'aws'
        self.test_create_cloud_service(cloud_service_type=cloud_service_type, provider=provider)

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'domain_id': self.domain.domain_id
        }
        self.cloud_service = self.inventory_v1.CloudService.get(param, metadata=(('token', self.owner_token),))
        self.assertEqual(self.cloud_service.provider, provider)

    def test_list_cloud_service_types(self):
        self.test_create_cloud_service()
        self.test_create_cloud_service()
        self.test_create_cloud_service()

        param = {
            'cloud_service_id': self.cloud_service.cloud_service_id,
            'domain_id': self.domain.domain_id
        }

        cloud_services = self.inventory_v1.CloudService.list(param, metadata=(('token', self.owner_token),))

        self.assertEqual(1, cloud_services.total_count)

    def test_list_cloud_services_cloud_service_type(self):
        self.test_create_cloud_service()
        self.test_create_cloud_service()
        self.test_create_cloud_service()
        self.test_create_cloud_service()
        self.test_create_cloud_service()
        self.test_create_cloud_service()

        param = {
            'cloud_service_type': self.cloud_service.cloud_service_type,
            'domain_id': self.domain.domain_id
        }

        cloud_svcs = self.inventory_v1.CloudService.list(param, metadata=(('token', self.owner_token),))

        self.assertEqual(1, cloud_svcs.total_count)

    def test_list_query(self):
        for x in range(0, 10):
            self.test_create_cloud_service()

        # self.test_create_cloud_service()
        # self.test_create_cloud_service()
        # self.test_create_cloud_service()
        # self.test_create_cloud_service()
        # self.test_create_cloud_service()
        # self.test_create_cloud_service()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    # {
                    #     'k': 'cloud_service_id',
                    #     'v': list(map(lambda cloud_service: cloud_service.cloud_service_id, self.cloud_services)),
                    #     'o': 'in'
                    # }
                    {
                        'k': 'domain_id',
                        'v': self.domain.domain_id,
                        'o': 'eq'
                    }
                ]
            }
        }

        cloud_services = self.inventory_v1.CloudService.list(param, metadata=(('token', self.owner_token),))
        self.assertEqual(len(self.cloud_services), cloud_services.total_count)

    def test_list_query_2(self):
        group = utils.random_string()

        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service()
        self.test_create_cloud_service()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {},
            'cloud_service_group': group
        }

        cloud_services = self.inventory_v1.CloudService.list(param, metadata=(('token', self.owner_token),))
        self.assertEqual(4, cloud_services.total_count)

    def test_list_query_minimal(self):
        group = utils.random_string()

        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service(group=group)
        self.test_create_cloud_service()
        self.test_create_cloud_service()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'minimal': True
            }
        }

        response = self.inventory_v1.CloudService.list(param, metadata=(('token', self.owner_token),))
        self.assertEqual(len(response.results), response.total_count)

    def test_stat_cloud_service(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': [{
                    'group': {
                        'keys': [{
                            'key': 'network_id',
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

        result = self.inventory_v1.CloudService.stat(
            params, metadata=(('token', self.owner_token),))

        self._print_data(result, 'test_stat_cloud_service')

    def test_stat_cloud_service_distinct(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'distinct': 'cloud_service_id',
                'page': {
                    'start': 1,
                    'limit': 3
                }
            }
        }

        result = self.inventory_v1.CloudService.stat(
            params, metadata=(('token', self.owner_token),))

        self._print_data(result, 'test_stat_cloud_service_distinct')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

