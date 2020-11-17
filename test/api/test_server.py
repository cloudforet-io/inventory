import os
import uuid
import unittest
import pprint
import random

from google.protobuf.json_format import MessageToDict
from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner


def random_string():
    return uuid.uuid4().hex


class TestServer(unittest.TestCase):
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
        super(TestServer, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})

        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestServer, cls).tearDownClass()
        cls.identity_v1.DomainOwner.delete({
            'domain_id': cls.domain.domain_id,
            'owner_id': cls.owner_id
        })
        print(f'>> delete domain owner: {cls.owner_id}')

        if cls.domain:
            cls.identity_v1.Domain.delete({'domain_id': cls.domain.domain_id})
            print(f'>> delete domain: {cls.domain.name} ({cls.domain.domain_id})')

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

        owner = cls.identity_v1.DomainOwner.create({
            'owner_id': cls.owner_id,
            'password': cls.owner_pw,
            'domain_id': cls.domain.domain_id
        })

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
        self.servers = []
        self.server = None
        self.regions = []
        self.region = None
        self.projects = []
        self.project = None
        self.project_groups = []
        self.project_group = None
        self.collectors = []
        self.collector = None
        self.secrets = []
        self.secret = None

    def tearDown(self):
        print()
        for server in self.servers:
            self.inventory_v1.Server.delete(
                {'server_id': server.server_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )
            print(f'>> delete server: {server.name} ({server.server_id})')

        for collector in self.collectors:
            self.inventory_v1.Collector.delete(
                {'collector_id': collector.collector_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )
            print(f'>> delete collector: {collector.name} ({collector.collector_id})')

        for region in self.regions:
            self.inventory_v1.Region.delete(
                {'region_id': region.region_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )
            print(f'>> delete region: {region.name} ({region.region_id})')

        for project in self.projects:
            self.identity_v1.Project.delete(
                {'project_id': project.project_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )
            print(f'>> delete project: {project.name} ({project.project_id})')

        for project_group in self.project_groups:
            self.identity_v1.ProjectGroup.delete(
                {'project_group_id': project_group.project_group_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )
            print(f'>> delete project group: {project_group.name} ({project_group.project_group_id})')

    def _create_project_group(self, name=None):
        if name is None:
            name = 'ProjectGroup-' + utils.random_string()[0:5]

        params = {
            'name': name,
            'tags': {'aa': 'bb'},
            'domain_id': self.domain.domain_id
        }

        self.project_group = self.identity_v1.ProjectGroup.create(
            params,
            metadata=(('token', self.token),)
        )

        self.project_groups.append(self.project_group)
        self.assertEqual(self.project_group.name, params['name'])

    def _create_project(self, project_group_id, name=None):
        if name is None:
            name = 'Project-' + utils.random_string()[0:5]

        params = {
            'name': name,
            'project_group_id': project_group_id,
            'tags': {'aa': 'bb'},
            'domain_id': self.domain.domain_id
        }

        self.project = self.identity_v1.Project.create(
            params,
            metadata=(('token', self.token),)
        )

        self.projects.append(self.project)
        self.assertEqual(self.project.name, params['name'])

    def _create_region(self, name=None, region_type='AWS', region_code=None):
        if name is None:
            name = 'Region-' + random_string()[0:5]

        if region_code is None:
            region_code = 'region-' + random_string()[0:5]

        params = {
            'name': name,
            'region_code': region_code,
            'region_type': region_type,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(
            params,
            metadata=(('token', self.token),))

        self.regions.append(self.region)
        self.assertEqual(self.region.name, params['name'])

    def _print_data(self, message, description=None):
        print()
        if description:
            print(f'[ {description} ]')

        self.pp.pprint(MessageToDict(message, preserving_proto_field_name=True))

    def test_create_server(self, name=None, **kwargs):
        """ Create Server
        """
        if name is None:
            name = 'Server-' + random_string()[0:5]

        self._create_project_group()
        self._create_project(self.project_group.project_group_id)
        self._create_region()

        ip_address = f'192.168.0.{random.randrange(1, 5)}'

        params = {
            'name': name,
            'primary_ip_address': ip_address,
            'os_type': 'LINUX',
            'provider': 'aws',
            'cloud_service_group': 'EC2',
            'cloud_service_type': 'Instance',
            "server_type": "VM",
            'data': {
                'os': {
                    'os_distro': 'ubuntu18.04',
                    'os_details': 'Ubuntu 18.04.2 LTS'
                },
                'hardware': {
                    'core': 4,
                    'memory': 8
                },
                'compute': {
                    'instance_id': 'i-' + random_string()[0:12]
                },
                'softwares': [{
                    'name': 'mysql',
                    'version': '1.0.0'
                }, {
                    'name': 'apache',
                    'version': '3.0.0'
                }, {
                    'name': 'nginx',
                    'version': '2.0.0'
                }]
            },
            'nics': [{
                'ip_addresses': [ip_address],
                'cidr': '192.168.0.0/24',
                'mac_address': 'aa:bb:cc:dd:ee:ff',
                'device': 'eth0',
                'device_index': 0,
                'public_ip_address': '1.1.1.1'
            }],
            'disks': [{
                'device_index': 0,
                'device': '/dev/sda',
                'size': 100.0,
                'disk_type': 'ebs'
            }],
            'metadata': {
                'view': {
                    'sub_data': {
                        'layouts': [{
                            'name': 'Hardware2',
                            'type': 'item',
                            'options': {
                                'fields': [{
                                    'key': 'data.hardware.core',
                                    'name': 'Core'
                                }, {
                                    'key': 'data.hardware.memory',
                                    'name': 'Memory'
                                }]
                            }
                        }, {
                            'name': 'Hardware3',
                            'type': 'item',
                            'options': {
                                'fields': [{
                                    'key': 'data.hardware.core',
                                    'name': 'Core'
                                }, {
                                    'key': 'data.hardware.memory',
                                    'name': 'Memory'
                                }]
                            }
                        }, {
                            'name': 'Compute',
                            'type': 'item',
                            'options': {
                                'fields': [{
                                    'key': 'data.compute.instance_id',
                                    'name': 'Instance ID'
                                }, {
                                    'key': 'data.platform.type',
                                    'name': 'Platform Type',
                                    'view_type': 'badge',
                                    'background_color': 'yellow'
                                }]
                            }
                        }]
                    }
                }
            },
            "reference": {
                "resource_id": utils.generate_id('resource'),
                "external_link": "https://aaa.bbb.ccc/"
            },
            'project_id': self.project.project_id,
            'region_code': self.region.region_code,
            'region_type': self.region.region_type,
            'domain_id': self.domain.domain_id,
            'tags': {
                'tag_key': 'tag_value'
            }
        }

        metadata = (('token', self.token),)
        ext_meta = kwargs.get('meta')

        if ext_meta:
            metadata += ext_meta

        self.server = self.inventory_v1.Server.create(
            params,
            metadata=metadata)

        self.servers.append(self.server)
        self._print_data(self.server, 'test_create_server')
        self.assertEqual(self.server.name, name)

    def test_create_server_by_collector(self, name=None):
        """ Create Server by Collector
        """
        self._create_project_group()
        self._create_project(project_group_id=self.project_group.project_group_id)
        self.test_create_server(name, meta=(
            ('job_id', utils.generate_id('job')),
            ('collector_id', utils.generate_id('collector')),
            ('plugin_id', utils.generate_id('plugin')),
            ('secret.secret_id', utils.generate_id('secret')),
            ('secret.service_account_id', utils.generate_id('sa')),
            ('secret.project_id', self.project.project_id),
            ('secret.provider', 'aws')
        ))

    def test_update_server_by_collector(self, name=None):
        """ Create Server by Collector
        """
        self.test_create_server_by_collector()

        self.server = self.inventory_v1.Server.update(
            {
                'server_id': self.server.server_id,
                'os_type': 'WINDOWS',
                'cloud_service_group': 'ComputeEngine',
                'cloud_service_type': 'Instance',
                'data': {
                    'hardware': {
                        'core': 8,
                        'memory': 16
                    },
                    'os': {
                        'os_distro': 'windows2012',
                        'os_details': 'Windows 2012 ENT SP2'
                    },
                    'iam': {
                        'profile': {
                            'k1': 'v1',
                            'k2': 'v2'
                        }
                    },
                    'lv1': {
                        'lv2': {
                            'lv3': {
                                'k1': 'v1',
                                'k2': 'v2'
                            }
                        }
                    }
                },
                'metadata': {
                    'view': {
                        'sub_data': {
                            'layouts': [{
                                'name': 'Hardware',
                                'type': 'item',
                                'options': {
                                    'fields': [{
                                        'key': 'data.hardware.core',
                                        'name': 'Core'
                                    }, {
                                        'key': 'data.hardware.memory',
                                        'name': 'Memory'
                                    }]
                                }
                            }, {
                                'name': 'Hardware3',
                                'type': 'item',
                                'options': {
                                    'fields': [{
                                        'key': 'data.hardware.core',
                                        'name': 'Core2'
                                    }, {
                                        'key': 'data.hardware.memory',
                                        'name': 'Memory'
                                    }]
                                }
                            }, {
                                'name': 'Compute',
                                'type': 'item',
                                'options': {
                                    'fields': [{
                                        'key': 'data.compute.instance_id',
                                        'name': 'Instance ID2'
                                    }, {
                                        'key': 'data.platform.type',
                                        'name': 'Platform Type',
                                        'view_type': 'badge',
                                        'background_color': 'yellow'
                                    }]
                                }
                            }]
                        }
                    }
                },
                "reference": {
                    "resource_id": "resource-yyyy",
                    "external_link": "https://ddd.eee.fff"
                },
                'domain_id': self.domain.domain_id
            },
            metadata=(
                ('token', self.token),
                ('job_id', utils.generate_id('job')),
                # ('collector_id', utils.generate_id('collector')),
                # ('plugin_id', utils.generate_id('plugin')),
                ('secret.secret_id', utils.generate_id('secret')),
                ('secret.service_account_id', utils.generate_id('sa')),
                ('secret.project_id', self.project.project_id),
                ('secret.provider', 'aws')
            ))

        self._print_data(self.server, 'test_update_server_by_collector_1')

        self.server = self.inventory_v1.Server.update(
            {
                'server_id': self.server.server_id,
                'data': {
                    'hardware': {
                        'core': 12
                    },
                    'route': {
                        'default_gateway': '192.168.0.1'
                    },
                    'compute': {
                        'instance_id': 'i-' + random_string()[0:12],
                        'changed_key': 'changed_value'
                    },
                    'softwares': [{
                        'name': 'mysql',
                        'version': '1.0.0'
                    }, {
                        'name': 'apache',
                        'version': '2.0.0'
                    }],
                    'platform': {
                        'type': 'AZURE'
                    },
                    'iam': {
                        'profile': {
                            'k2': 'v2',
                            'k3': 'v3'
                        }
                    },
                    'lv1': {
                        'lv2': {
                            'lv3': {
                                'k2': 'v2',
                                'k3': 'v3'
                            }
                        }
                    }
                },
                'server_type': 'BAREMETAL',
                "metadata": {
                    'view': {
                        'sub_data': {
                            'layouts': [{
                                'name': 'Compute',
                                'type': 'item',
                                'options': {
                                    'fields': [{
                                        'key': 'data.compute.instance_id',
                                        'name': 'Instance ID2'
                                    }, {
                                        'key': 'data.platform.type',
                                        'name': 'Platform Type2',
                                        'view_type': 'badge',
                                        'background_color': 'yellow'
                                    }]
                                }
                            }]
                        }
                    }
                },
                'domain_id': self.domain.domain_id
            },
            metadata=(
                ('token', self.token),
                ('job_id', utils.generate_id('job')),
                # ('plugin_id', utils.generate_id('plugin')),
                # ('collector_id', utils.generate_id('collector')),
                ('secret.secret_id', utils.generate_id('secret')),
                ('secret.service_account_id', utils.generate_id('sa')),
                ('secret.project_id', self.project.project_id),
                ('secret.provider', 'aws'),
                ('update_mode', 'MERGE')
            ))

        server_data = MessageToDict(self.server, preserving_proto_field_name=True)

        self._print_data(self.server, 'test_update_server_by_collector_2')
        # self.assertEqual(server_data['os_type'], 'WINDOWS')
        # self.assertEqual(server_data['data']['hardware']['core'], 8)
        # self.assertEqual(server_data['reference']['resource_id'], 'resource-yyyy')
        # self.assertEqual(server_data['data']['platform']['type'], 'AWS')
        # self.assertEqual(server_data['data']['softwares'][0]['name'], 'mysql')
        # self.assertEqual(server_data['server_type'], 'BAREMETAL')

    def test_pin_server_data(self, name=None):
        """ Create Server by Collector
        """
        self.test_create_server(name)

        self.server = self.inventory_v1.Server.pin_data(
            {
                'server_id': self.server.server_id,
                'keys': [
                    'os_type',
                    'data.hardware',
                ],
                'domain_id': self.domain.domain_id
            },
            metadata=(
                ('token', self.token),
            ))

        self._print_data(self.server, 'test_pin_server_data_1')

        self._create_region()
        self.server = self.inventory_v1.Server.update(
            {
                'server_id': self.server.server_id,
                'os_type': 'WINDOWS',
                'data': {
                    'hardware': {
                        'core': 8,
                        'manufacturer': 'HP',
                        'memory': 16
                    },
                    'os': {
                        'os_distro': 'windows2012',
                        # 'os_details': 'Windows 2012 ENT SP2'
                    }
                },
                'nics': [{
                    'ip_addresses': ['192.168.0.3'],
                    'cidr': '192.168.0.0/24',
                    'mac_address': 'aa:bb:cc:dd:ee:ff',
                    'device': 'eth0',
                    'device_index': 0,
                    'public_ip_address': '1.1.1.1'
                }],
                'domain_id': self.domain.domain_id
            },
            metadata=(
                ('token', self.token),
                ('collector_id', utils.generate_id('collector')),
                ('service_account_id', utils.generate_id('sa')),
                ('secret_id', utils.generate_id('secret')),
                ('job_id', utils.generate_id('job')),
            ))

        server_data = MessageToDict(self.server, preserving_proto_field_name=True)

        self._print_data(self.server, 'test_pin_server_data_2')
        self.assertEqual(server_data['os_type'], 'LINUX')
        self.assertEqual(server_data['data']['hardware']['core'], 4)
        self.assertEqual(server_data['data']['os']['os_distro'], 'windows2012')

    def test_update_server_name(self, name=None):
        if name is None:
            name = 'UpdateServer-' + random_string()[0:5]

        self.test_create_server()

        params = {
            'server_id': self.server.server_id,
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))
        self.assertEqual(self.server.name, name)

    def test_update_server_data(self):
        self.test_create_server()

        params = {
            'server_id': self.server.server_id,
            'data': {
                'base': {
                    'core': 8,
                    'memory': 16
                }
            },
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_update_server_data')
        self.assertEqual(self.server.data['base'], params['data']['base'])

    def test_update_server_region(self):
        self.test_create_server()

        params = {
            'server_id': self.server.server_id,
            'region_code': self.region.region_code,
            'region_type': self.region.region_type,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_update_server_region')
        self.assertEqual(self.server.region_code, self.region.region_code)

    def test_release_server_region(self):
        self.test_create_server()

        params = {
            'server_id': self.server.server_id,
            'release_region': True,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_release_server_pool')
        self.assertEqual(self.server.region_code, '')

    def test_update_server_project(self):
        self.test_create_server()
        self._create_project(self.project_group.project_group_id)

        params = {
            'server_id': self.server.server_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_update_server_project')
        self.assertEqual(self.server.project_id, self.project.project_id)

    def test_release_server_project(self):
        self.test_create_server()
        self._create_project(self.project_group.project_group_id)

        params = {
            'server_id': self.server.server_id,
            'project_id': self.project.project_id,
            'release_project': True,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_release_server_project')
        self.assertEqual(self.server.project_id, '')

    def test_update_server_tags(self, tags=None):
        self.test_create_server()

        if tags is None:
            tags = {
                random_string(): random_string(),
                random_string(): random_string()
            }

        params = {
            'server_id': self.server.server_id,
            'tags': tags,
            'domain_id': self.domain.domain_id
        }

        self.server = self.inventory_v1.Server.update(
            params,
            metadata=(('token', self.token),))

        self._print_data(self.server, 'test_update_server_tags')
        self.assertEqual(MessageToDict(self.server.tags, preserving_proto_field_name=True), tags)

    def test_get_server(self, name=None):
        if name is None:
            name = 'GetServer-' + random_string()[0:5]

        self.test_create_server(name)

        params = {
            'server_id': self.server.server_id,
            'domain_id': self.domain.domain_id
        }
        self.server = self.inventory_v1.Server.get(
            params,
            metadata=(('token', self.token),))

        self.assertEqual(self.server.name, name)

    def test_list_server_id(self):
        self.test_create_server()
        self.test_create_server()

        params = {
            'server_id': self.server.server_id,
            'domain_id': self.domain.domain_id
        }

        result = self.inventory_v1.Server.list(
            params,
            metadata=(('token', self.token),))

        self.assertEqual(1, result.total_count)

    def test_list_server_name(self):
        name = 'ListServers-' + random_string()[0:5]

        self.test_create_server(name)
        self.test_create_server(name)

        params = {
            'name': self.server.name,
            'domain_id': self.domain.domain_id
        }

        result = self.inventory_v1.Server.list(
            params,
            metadata=(('token', self.token),))

        self.assertEqual(2, result.total_count)

    def test_list_query(self):
        self.test_create_server(name='server-123')
        self.test_create_server(name='server-123')
        self.test_create_server(name='server-123')
        self.test_create_server(name='server-23')
        self.test_create_server(name='server-23')

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'server_id',
                        'v': list(map(lambda server: server.server_id, self.servers)),
                        'o': 'in'
                    }
                ]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))

        self.assertEqual(len(self.servers), result.total_count)

    def test_list_match_query(self):
        self.test_create_server()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'data.softwares',
                        'v': {
                            'name': 'mysql',
                            'version': {
                               '$ne': '2.0.0'
                            }
                        },
                        'o': 'match'
                    }
                ]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))
        self.assertEqual(len(self.servers), result.total_count)

    def test_list_region_code(self):
        self.test_create_server()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'region_code',
                        'v': self.region.region_code,
                        'o': 'eq'
                    }
                ]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))
        self.assertEqual(len(self.servers), result.total_count)

    def test_list_minimal(self):
        self.test_create_server()
        self.test_create_server()
        self.test_create_server()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'minimal': True
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))
        self.assertEqual(len(self.servers), result.total_count)

    def test_list_server_specific_field(self):
        self.test_create_server()

        params = {
            'domain_id': self.domain.domain_id,
            'server_id': self.server.server_id,
            'query': {
                'only': ['server_id', 'name', 'state'],
                'filter': [{
                    'k': 'state',
                    'v': 'DELETE',
                    'o': 'not'
                }]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))

        self._print_data(result.results[0], 'test_list_server_specific_field')
        self.assertEqual(self.server.server_id, result.results[0].server_id)

    def test_list_server_by_timediff(self):
        self.test_create_server()

        params = {
            'domain_id': self.domain.domain_id,
            'server_id': self.server.server_id,
            'query': {
                'minimal': True,
                'filter': [{
                    'k': 'created_at',
                    'v': 'now/w - 3h',
                    'o': 'timediff_gt'
                }]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_list_server_by_timediff')
        self.assertEqual(result.total_count, 1)

    def test_list_server_deleted(self):
        self.test_create_server()

        for server in self.servers:
            self.inventory_v1.Server.delete(
                {'server_id': server.server_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        params = {
            'domain_id': self.domain.domain_id,
            'server_id': self.server.server_id,
            'query': {
                'minimal': True,
                'filter': [{
                    'key': 'state',
                    'value': 'DELETED',
                    'operator': 'eq'
                }]
            }
        }

        result = self.inventory_v1.Server.list(
            params, metadata=(('token', self.token),))

        self.servers = []

        self._print_data(result, 'test_list_server_deleted')
        self.assertEqual(result.total_count, 1)

    def test_stat_server(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'name',
                            'name': 'Name'
                        }, {
                            'key': 'provider',
                            'name': 'Provider'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Server Count'
                        }, {
                            'key': 'data.hardware.core',
                            'operator': 'sum',
                            'name': 'Total Core'
                        }, {
                            'k': 'data.hardware.memory',
                            'o': 'max',
                            'n': 'Max Server Memory'
                        }, {
                            'key': 'reference.resource_id',
                            'operator': 'add_to_set',
                            'name': 'All Reference Resource ID'
                        }]
                    }
                },
                'sort': {
                    'name': 'Server Count',
                    'desc': True
                },
                'page': {
                    'start': 2,
                    'limit': 1
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server')

    def test_stat_server_by_ip_addresses(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'unwind': [{
                       'path': 'nics'
                    }, {
                        'path': 'nics.ip_addresses'
                    }],
                    'group': {
                        'keys': [{
                            'key': 'nics.ip_addresses.ip_address',
                            'name': 'ip_address'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Server Count'
                        }, {
                            'key': 'data.hardware.core',
                            'operator': 'sum',
                            'name': 'Total Core'
                        }, {
                            'k': 'data.hardware.memory',
                            'o': 'max',
                            'n': 'Max Server Memory'
                        }, {
                            'key': 'reference.resource_id',
                            'operator': 'add_to_set',
                            'name': 'All Reference Resource ID'
                        }]
                    }
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server_by_collector')

    def test_stat_server_by_service_account(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'unwind': [{
                        "path": "collection_info.service_accounts"
                    }],
                    'group': {
                        'keys': [{
                            'key': 'collection_info.service_accounts',
                            'name': 'service_account_id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Server Count'
                        }]
                    }
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server_by_service_account')

    def test_stat_server_vm_count(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                "filter": [{
                    "key": "server_type",
                    "value": "VM",
                    "operator": "eq"
                }],
                'aggregate': {
                    'count': {
                        'name': 'VM Server Count'
                    }
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server_vm_count')

    def test_stat_server_created_time(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'created_at',
                            'name': 'created_at'
                        }, {
                            'key': 'project_id',
                            'name': 'project_id'
                        }],
                        'fields': [{
                            'operator': 'count',
                            'name': 'Server Total Count'
                        }]
                    }
                },
                'page': {
                    'start': 2,
                    'limit': 3
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server_total_count')

    def test_stat_server_distinct(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'distinct': 'server_id',
                'page': {
                    'start': 2,
                    'limit': 3
                }
            }
        }

        result = self.inventory_v1.Server.stat(
            params, metadata=(('token', self.token),))

        self._print_data(result, 'test_stat_server_distinct')


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
