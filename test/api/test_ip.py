import os
import unittest
import uuid

from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner
from google.protobuf.json_format import MessageToDict


def random_string():
    return uuid.uuid4().hex


class TestIP(unittest.TestCase):
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
    api_key_obj = None

    @classmethod
    def setUpClass(cls):
        super(TestIP, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestIP, cls).tearDownClass()

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
        self.subnet = None
        self.ip = None
        self.ips = []

        self._create_zone()

    def tearDown(self):
        for ip in self.ips:
            self.inventory_v1.IPAddress.release(
                {'ip_address': ip.ip_address,
                 'subnet_id': self.subnet.subnet_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        self.inventory_v1.Subnet.delete(
            {'subnet_id': self.subnet.subnet_id,
             'domain_id': self.domain.domain_id},
            metadata=(('token', self.token),)
            )

        self.inventory_v1.Network.delete({'network_id': self.network.network_id,
                                          'domain_id': self.domain.domain_id},
                                         metadata=(('token', self.token),))

        self.inventory_v1.NetworkType.delete({'network_type_id': self.network_type.network_type_id,
                                              'domain_id': self.domain.domain_id},
                                             metadata=(('token', self.token),))

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

    def _create_subnet(self, name=None, cidr=None, ip_ranges=None, network_id=None, network_cidr=None,
                       network_type_id=None, gateway=None, vlan=None, data=None):

        if not name:
            name = random_string()

        if not cidr:
            cidr = '172.16.1.0/24'

        if not network_type_id:
            self._create_network_type()
            network_type_id = self.network_type.network_type_id

        if not network_id:
            self._create_network(cidr=network_cidr)
            network_id = self.network.network_id

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

        if gateway is not None:
            params.update({
                'gateway': gateway
            })

        if vlan is not None:
            params.update({
                'vlan': vlan
            })

        if data is not None:
            params.update({
                'data': data
            })

        self.subnet = self.inventory_v1.Subnet.create(params,
                                                      metadata=(('token', self.token),))

    def test_allocate_ip(self, subnet_id=None, ip=None):
        if subnet_id is None:
            self._create_subnet()
            subnet_id = self.subnet.subnet_id

        params = {
            'subnet_id': subnet_id,
            'domain_id': self.domain.domain_id

        }

        if ip is not None:
            params.update({
                'ip_address': ip
            })

        self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                       metadata=(('token', self.token),))

        self.ips.append(self.ip)

        print(self.ip)
        self.assertEqual(self.ip, self.ip)

    def test_allocate_ip_multiple(self, num=10):
        self._create_subnet()

        params = {
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id

        }

        for i in range(num):
            self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                           metadata=(('token', self.token),))
            print(self.ip.ip_address)
            self.ips.append(self.ip)

        self.assertEqual(self.ip, self.ip)

    def test_allocate_ip_2(self):
        self.test_allocate_ip_multiple(num=5)
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.7')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.9')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.10')

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.6
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        ip = self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.8
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        ip = self.test_allocate_ip(subnet_id=self.subnet.subnet_id) # 172.16.1.11
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.assertEqual(self.ip, self.ip)

    def test_allocate_ip_3(self):
        # Allocate
        self.test_allocate_ip_multiple(num=5)
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.7')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.9')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.10')

        # Reserve
        self.test_reserve_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.12')

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.6
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        ip = self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.8
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        ip = self.test_allocate_ip(subnet_id=self.subnet.subnet_id) # 172.16.1.11
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        ip = self.test_allocate_ip(subnet_id=self.subnet.subnet_id) # 172.16.1.13
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.assertEqual(self.ip, self.ip)

    def test_allocate_ip_4(self):
        # Allocate
        ip_ranges = [
            {'start': '172.16.1.10',
             'end': '172.16.1.20'}
        ]

        self._create_subnet(ip_ranges=ip_ranges)
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.10
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.11
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.12
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.13
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.14
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.assertEqual(self.ip, self.ip)

    def test_allocate_ip_full(self):
        # Allocate
        ip_ranges = [
            {'start': '172.16.1.10',
             'end': '172.16.1.13'}
        ]

        self._create_subnet(ip_ranges=ip_ranges)
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.10
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.11
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.12
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.13
        print("------------")
        print(self.ip.ip_address)
        print("------------")

        with self.assertRaises(Exception):
            self.test_allocate_ip(subnet_id=self.subnet.subnet_id)  # 172.16.1.14

    def test_allocate_ip_resource(self):
        self._create_subnet()

        resource = {
            'type': 'server',
            'id': 'server-xxxxx'
        }

        params = {
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id,
            'resource': resource
        }
        self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                       metadata=(('token', self.token),))

        self.ips.append(self.ip)
        self.assertEqual(MessageToDict(self.ip.resource), resource)

    def test_allocate_ip_static(self):
        self._create_subnet()

        ip = '172.16.1.10'
        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id

        }
        self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                       metadata=(('token', self.token),))

        self.ips.append(self.ip)
        self.assertEqual(self.ip.ip_address, ip)

    def test_allocate_ip_static_duplicate(self):
        self._create_subnet()

        ip = '172.16.1.10'
        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id

        }
        self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                       metadata=(('token', self.token),))

        self.ips.append(self.ip)

        with self.assertRaises(Exception):
            self.inventory_v1.IPAddress.allocate(params,
                                       metadata=(('token', self.token),))

    def test_allocate_ip_reserve_duplicate(self):
        self._create_subnet()

        ip = '172.16.1.11'
        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id

        }
        self.ip = self.inventory_v1.IPAddress.reserve(params, metadata=(('token', self.token),))
        self.ip = self.inventory_v1.IPAddress.allocate(params, metadata=(('token', self.token),))
        self.ips.append(self.ip)

        self.assertEqual(self.ip.state, 1)

    def test_allocate_ip_data(self):
        self._create_subnet()

        data = {
            'ec2': {
                'instance_id': 'i-xxxxxxxx'
            }
        }

        params = {
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id,
            'data': data

        }
        self.ip = self.inventory_v1.IPAddress.allocate(params,
                                                       metadata=(('token', self.token),))

        self.ips.append(self.ip)
        self.assertEqual(MessageToDict(self.ip.data), data)

    def test_allocate_ip_duplicate(self):
        self.test_allocate_ip()

        ip = self.ip.ip_address
        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id

        }

        with self.assertRaises(Exception):
            self.inventory_v1.IPAddress.allocate(params, metadata=(('token', self.token),))

    def test_reserve_ip(self, subnet_id=None, ip=None):
        if subnet_id is None:
            self._create_subnet()
            subnet_id = self.subnet.subnet_id

        if ip is None:
            ip = '172.16.1.1'

        params = {
            'ip_address': ip,
            'subnet_id': subnet_id,
            'domain_id': self.domain.domain_id

        }

        self.ip = self.inventory_v1.IPAddress.reserve(params,
                                                      metadata=(('token', self.token),))

        self.ips.append(self.ip)
        self.assertEqual(self.ip.state, 2)

    def test_update_ip_resource(self):
        self._create_subnet()

        ip = '172.16.1.10'
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip=ip)

        resource = {
            'type': 'server',
            'id': 'server-xxxxx'
        }

        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id,
            'resource': resource
        }
        self.ip = self.inventory_v1.IPAddress.update(params,
                                                     metadata=(('token', self.token),))

        self.assertEqual(MessageToDict(self.ip.resource), resource)

    def test_update_ip_data(self):
        self._create_subnet()

        ip = '172.16.1.10'
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip=ip)

        data = {
            'xxxxx': 'bbbb',
            'yyyyyy': 'zzzz',
            'aaaaa': {
                'bbbbb': 'cccccc'
            }
        }

        params = {
            'ip_address': ip,
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id,
            'data': data
        }
        self.ip = self.inventory_v1.IPAddress.update(params,
                                                     metadata=(('token', self.token),))

        self.assertEqual(MessageToDict(self.ip.data), data)

    def test_get_ip(self):
        self._create_subnet()
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.10')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.11')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.12')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.13')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.14')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.15')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.16')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.17')

        param = {
            'ip_address': '172.16.1.13',
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id
        }
        ip = self.inventory_v1.IPAddress.get(param,
                                             metadata=(('token', self.token),))

        self.assertEqual(ip.ip_address, '172.16.1.13')

    def test_list_ips(self):
        self._create_subnet()
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.10')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.11')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.12')
        self.test_allocate_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.13')
        self.test_reserve_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.14')
        self.test_reserve_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.15')
        self.test_reserve_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.16')
        self.test_reserve_ip(subnet_id=self.subnet.subnet_id, ip='172.16.1.17')

        param = {
            'state': 'ALLOCATED',
            'subnet_id': self.subnet.subnet_id,
            'domain_id': self.domain.domain_id
        }
        ips = self.inventory_v1.IPAddress.list(param, metadata=(('token', self.token),))

        self.assertEqual(ips.total_count, 4)

    def test_stat_ip(self):
        self.test_list_ips()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'ip_address',
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

        result = self.inventory_v1.IPAddress.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

