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


class TestSubnet(unittest.TestCase):
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
        super(TestSubnet, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestSubnet, cls).tearDownClass()
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
        self.network = None
        self.network_type = None
        self.network_types = []
        self.network_policy = None
        self.network_polcies = []
        self.subnets = []
        self.subnet = None
        self.project_group = None
        self.project = None
        self.projects = []

        self._create_zone()

    def tearDown(self):
        for subnet in self.subnets:
            self.inventory_v1.Subnet.delete(
                {'subnet_id': subnet.subnet_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        if self.network:
            self.inventory_v1.Network.delete(
                {'network_id': self.network.network_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        for ntype in self.network_types:
            self.inventory_v1.NetworkType.delete(
                {'network_type_id': ntype.network_type_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        for npolicy in self.network_polcies:
            self.inventory_v1.NetworkPolicy.delete(
                {'network_policy_id': npolicy.network_policy_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        for project in self.projects:
            self.identity_v1.Project.delete(
                {'project_id': project.project_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        if self.project_group:
            self.identity_v1.ProjectGroup.delete(
                {'project_group_id': self.project_group.project_group_id,
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

    def _create_network_type(self):
        params = {
            'name': random_string(),
            'domain_id': self.domain.domain_id
        }

        self.network_type = self.inventory_v1.NetworkType.create(params,
                                                                 metadata=(('token', self.token),)
                                                                 )

        self.network_types.append(self.network_type)

    def _create_network_policy(self, name=None):
        if not name:
            name = random_string()

        params = {
            'name': name,
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        self.network_policy = self.inventory_v1.NetworkPolicy.create(params,
                                                                     metadata=(('token', self.token),)
                                                                     )

        self.network_polcies.append(self.network_policy)

    def _create_project_group(self, name=None):
        if not name:
            name = random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.project_group = self.identity_v1.ProjectGroup.create(params,
                                                                  metadata=(('token', self.token),)
                                                                  )

    def _create_project(self, name=None, project_group_id=None):
        if not project_group_id:
            self._create_project_group()
            project_group_id = self.project_group.project_group_id

        if not name:
            name = random_string()

        params = {
            'name': name,
            'project_group_id': project_group_id,
            'domain_id': self.domain.domain_id
        }

        self.project = self.identity_v1.Project.create(params,
                                                       metadata=(('token', self.token),)
                                                      )

        self.projects.append(self.project)

    def _create_network(self, name=None, cidr=None):
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

        self.network = self.inventory_v1.Network.create(params,
                                                        metadata=(('token', self.token),)
                                                        )

    def test_create_subnet(self, name=None, cidr=None, ip_ranges=None, network_id=None, network_cidr=None,
                           network_type_id=None, network_policy_id=None, gateway=None, vlan=None, project_id=None, data=None):

        if not name:
            name = random_string()

        if not cidr:
            cidr = '172.16.1.0/24'

        if not network_id:
            self._create_network(cidr=network_cidr)
            network_id = self.network.network_id

        if not network_type_id:
            self._create_network_type()
            network_type_id = self.network_type.network_type_id

        params = {
            'name': name,
            'cidr': cidr,
            'network_id': network_id,
            'network_type_id': network_type_id,
            'domain_id': self.domain.domain_id
        }

        if ip_ranges is not None:
            params.update({
                'ip_ranges': ip_ranges
            })

        if network_policy_id is not None:
            params.update({
                'network_policy_id': network_policy_id
            })

        if gateway is not None:
            params.update({
                'gateway': gateway
            })

        if vlan is not None:
            params.update({
                'vlan': vlan
            })

        if project_id is not None:
            params.update({
                'project_id': project_id
            })

        if data is not None:
            params.update({
                'data': data
            })

        self.subnet = self.inventory_v1.Subnet.create(params,
                                                      metadata=(('token', self.token),))

        self.subnets.append(self.subnet)
        self.assertEqual(self.subnet.cidr, cidr)

    def test_create_subnet_with_cidr(self):
        cidr = '172.16.1.0/25'
        self.test_create_subnet(cidr=cidr)
        self.assertEqual(self.subnet.cidr, cidr)

    def test_create_subnet_with_ip_ranges(self):
        cidr = '172.16.1.0/25'
        ip_ranges = [{'start': '172.16.1.1', 'end': '172.16.1.2'}]

        self.test_create_subnet(cidr=cidr, ip_ranges=ip_ranges)
        self.assertEqual(self.subnet.cidr, cidr)

    def test_create_subnet_with_ip_ranges_2(self):
        cidr = '172.16.1.0/25'
        ip_ranges = [{'start': '172.16.1.1', 'end': '172.16.1.2'},
                     {'start': '172.16.1.10', 'end': '172.16.1.20'}]

        self.test_create_subnet(cidr=cidr, ip_ranges=ip_ranges)
        self.assertEqual(self.subnet.cidr, cidr)

    def test_create_subnet_with_vlan(self):
        vlan = 100

        self.test_create_subnet(vlan=vlan)
        self.assertEqual(self.subnet.vlan, vlan)

    def test_create_subnet_with_gw(self):
        gw = "172.16.1.254"

        self.test_create_subnet(gateway=gw)
        self.assertEqual(self.subnet.gateway, gw)

    def test_create_subnet_with_npolicy(self):
        self._create_network_policy()

        self.test_create_subnet(network_policy_id=self.network_policy.network_policy_id)
        self.assertEqual(self.subnet.network_policy_info.network_policy_id, self.network_policy.network_policy_id)

    def test_create_subnet_with_project(self):
        self._create_project()

        self.test_create_subnet(project_id=self.project.project_id)
        self.assertEqual(self.subnet.project_id, self.project.project_id)

    def test_create_subnet_with_data(self):
        data = {
            'vpc': {
                'vpc_id': 'vpc-xxxxxxx'
            }
        }
        self.test_create_subnet(data=data)
        self.assertEqual(MessageToDict(self.subnet.data), data)

    def test_create_subnet_invalid_cidr(self):
        with self.assertRaises(Exception):
            self.test_create_subnet(cidr='192.168.1.0/24')

    def test_create_subnet_invalid_network(self):
        with self.assertRaises(Exception):
            self.test_create_subnet(network_id='test')

    def test_create_subnet_invalid_ip_range(self):
        ip_ranges = [{'start': '192.168.0.1', 'end': '192.168.0.2'}]
        with self.assertRaises(Exception):
            self.test_create_subnet(ip_ranges=ip_ranges)

    def test_create_subnet_invalid_ip_range_2(self):
        cidr = '172.16.1.0/25'
        ip_ranges = [{'start': '172.16.1.10', 'end': '172.16.1.129'}]
        with self.assertRaises(Exception):
            self.test_create_subnet(cidr=cidr, ip_ranges=ip_ranges)

    def test_create_subnet_invalid_ip_range_3(self):
        cidr = '172.16.1.0/25'
        ip_ranges = [{'start': '172.16.1.1', 'end': '172.16.1.2'},
                     {'start': '172.16.1.11', 'end': '172.16.1.290'}]

        with self.assertRaises(Exception):
            self.test_create_subnet(cidr=cidr, ip_ranges=ip_ranges)

    def test_create_subnet_invalid_ip_range_4(self):
        cidr = '172.16.1.0/25'
        ip_ranges = [{'start': '172.16.1.1', 'end': '172.16.1.2'},
                     {'start': '172.16.1.20', 'end': '172.16.1.9'}]

        with self.assertRaises(Exception):
            self.test_create_subnet(cidr=cidr, ip_ranges=ip_ranges)

    def test_create_subnet_invalid_vlan(self):
        vlan = 20000
        with self.assertRaises(Exception):
            self.test_create_subnet(vlan=vlan)

    def test_create_subnet_invalid_gw(self):
        gw = "172.16.1.22222"
        with self.assertRaises(Exception):
            self.test_create_subnet(gateway=gw)

    def test_create_subnet_invalid_npolicy(self):
        npolicy = 'test'
        with self.assertRaises(Exception):
            self.test_create_subnet(network_policy_id=npolicy)

    def test_create_subnet_invalid_project(self):
        project = 'test'
        with self.assertRaises(Exception):
            self.test_create_subnet(project_id=project)

    def test_create_subnet_duplicate_cidr(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        with self.assertRaises(Exception):
            self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

    def test_create_subnet_duplicate_cidr_2(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        with self.assertRaises(Exception):
            self.test_create_subnet(cidr='172.16.1.0/25', network_id=self.network.network_id)

    def test_update_subnet_name(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        name = 'test-update-subnet'
        param = {
            'subnet_id': self.subnet.subnet_id,
            'name': name,
            'domain_id': self.domain.domain_id
        }
        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.name, name)

    def test_update_subnet_ip_ranges(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        ip_ranges = [
            {'start': '172.16.1.1', 'end': '172.16.1.10'}
        ]

        param = {
            'subnet_id': self.subnet.subnet_id,
            'ip_ranges': ip_ranges,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)

                                                      )

        self.assertEqual(self.subnet.subnet_id, self.subnet.subnet_id)

    def test_update_subnet_tags(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }

        param = {'subnet_id': self.subnet.subnet_id,
                 'tags': tags,
                 'domain_id': self.domain.domain_id,
                 }
        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(MessageToDict(self.subnet.tags), tags)

    def test_update_subnet_data(self):
        self.test_create_subnet()

        data = {
            'vpc': {
                'vpc_id': 'vpc-xxxxxxx'
            }
        }

        param = {
            'subnet_id': self.subnet.subnet_id,
            'data': data,
            'domain_id': self.domain.domain_id,
        }
        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(MessageToDict(self.subnet.data), data)

    def test_update_subnet_gw(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        gw = '172.16.1.254'

        param = {
            'subnet_id': self.subnet.subnet_id,
            'gateway': gw,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.gateway, gw)

    def test_update_subnet_vlan(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        vlan = 1000

        param = {
            'subnet_id': self.subnet.subnet_id,
            'vlan': vlan,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.vlan, vlan)

    def test_update_subnet_network_type(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self._create_network_type()

        param = {
            'subnet_id': self.subnet.subnet_id,
            'network_type_id': self.network_type.network_type_id,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.network_type_info.network_type_id, self.network_type.network_type_id)

    def test_update_subnet_network_policy(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self._create_network_policy()

        param = {
            'subnet_id': self.subnet.subnet_id,
            'network_policy_id': self.network_policy.network_policy_id,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.network_policy_info.network_policy_id, self.network_policy.network_policy_id)

    def test_update_subnet_project(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self._create_project()

        param = {
            'subnet_id': self.subnet.subnet_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.project_id, self.project.project_id)

    def test_update_subnet_release_project(self):
        self._create_project()
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24',
                                network_id=self.network.network_id,
                                project_id=self.project.project_id)

        param = {
            'subnet_id': self.subnet.subnet_id,
            'release_project': True,
            'domain_id': self.domain.domain_id
        }

        self.subnet = self.inventory_v1.Subnet.update(param,
                                                      metadata=(('token', self.token),)
                                                      )
        self.assertEqual(self.subnet.project_id, '')

    def test_get_subnet(self):
        name = 'test-subnet'
        self.test_create_subnet(name=name)

        param = {
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id
        }
        self.subnet = self.inventory_v1.Subnet.get(param,
                                                   metadata=(('token', self.token),)
                                                   )

        self.assertEqual(self.subnet.name, name)

    def test_list_subnet_id(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        param = {
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id
        }

        subnet = self.inventory_v1.Subnet.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, subnet.total_count)

    def test_list_subnet_name(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)

        name = 'test-subnet'
        self.test_create_subnet(name=name, cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(name=name, cidr='172.16.3.0/24', network_id=self.network.network_id)

        param = {
            'name': self.subnet.name,
            'domain_id': self.domain.domain_id
        }

        subnet = self.inventory_v1.Subnet.list(param, metadata=(('token', self.token),))

        self.assertEqual(2, subnet.total_count)

    def test_list_subnet_network_id(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        param = {
            'network_id': self.network.network_id,
            'domain_id': self.domain.domain_id
        }

        subnet = self.inventory_v1.Subnet.list(param, metadata=(('token', self.token),))

        self.assertEqual(3, subnet.total_count)

    def test_list_subnet_zone_id(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        subnet = self.inventory_v1.Subnet.list(param, metadata=(('token', self.token),))

        self.assertEqual(3, subnet.total_count)

    def test_list_subnets_query(self):
        self._create_network(cidr='172.16.0.0/16')
        self.test_create_subnet(cidr='172.16.1.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.2.0/24', network_id=self.network.network_id)
        self.test_create_subnet(cidr='172.16.3.0/24', network_id=self.network.network_id)

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'network_id',
                        'v': self.network.network_id,
                        'o': 'eq'
                    }
                ]
            }
        }

        subnets = self.inventory_v1.Subnet.list(param, metadata=(('token', self.token),))
        self.assertEqual(3, subnets.total_count)

    def test_stat_subnet(self):
        self.test_list_subnets_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'subnet_id',
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

        result = self.inventory_v1.Subnet.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
