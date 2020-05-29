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


class TestZone(unittest.TestCase):
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
        super(TestZone, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})
        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestZone, cls).tearDownClass()
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
        for zone in self.zones:
            self.inventory_v1.Zone.delete(
                {'zone_id': zone.zone_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        for region in self.regions:
            self.inventory_v1.Region.delete(
                {'region_id': region.region_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

        for user in self.users:
            self.identity_v1.User.delete(
                {'user_id': user.user_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.token),)
            )

    def _create_user(self, user_id=None):
        lang_code = random.choice(['zh-hans', 'jp', 'ko', 'en', 'es'])
        language = Language.get(lang_code)
        user_id = utils.random_string()[0:10] if user_id is None else user_id

        param = {
            'user_id': user_id,
            'domain_id': self.domain.domain_id,
            'password': 'qwerty123',
            'name': 'Steven' + utils.random_string()[0:5],
            'language': language.__str__(),
            'timezone': 'Asia/Seoul',
            'tags': {'aa': 'bb'},
            'email': 'Steven' + utils.random_string()[0:5] + '@mz.co.kr',
            'mobile': '+821026671234',
            'group': 'group-id',
        }

        user = self.identity_v1.User.create(
            param,
            metadata=(('token', self.token),)
        )
        self.user = user
        self.users.append(user)
        self.assertEqual(self.user.name, param['name'])

    def _create_region(self, name=None):
        """ Create Region
        """

        if not name:
            name = random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.region = self.inventory_v1.Region.create(params,
                                                      metadata=(('token', self.token),)
                                                      )

        self.regions.append(self.region)

    def test_create_zone(self, name=None, region_id=None):
        """ Create Zone
        """
        if region_id is None:
            self._create_region()
            region_id = self.region.region_id

        if not name:
            name = random_string()

        params = {
            'name': name,
            'region_id': region_id,
            'domain_id': self.domain.domain_id
        }
        self.zone = self.inventory_v1.Zone.create(params,
                                                  metadata=(('token', self.token),)
                                                  )

        self.zones.append(self.zone)
        self.assertEqual(self.zone.name, name)

    def test_update_zone_name(self):
        self.test_create_zone()

        name = random_string()
        param = { 'zone_id': self.zone.zone_id,
                  'name': name,
                  'domain_id': self.domain.domain_id,
                }
        self.zone = self.inventory_v1.Zone.update(param,
                                                  metadata=(('token', self.token),)
                                                  )
        self.assertEqual(self.zone.name, name)

    def test_update_zone_tags(self):
        self.test_create_zone()

        tags = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        param = { 'zone_id': self.zone.zone_id,
                  'tags': tags,
                  'domain_id': self.domain.domain_id,
                }
        self.zone = self.inventory_v1.Zone.update(param,
                                                    metadata=(('token', self.token),)
                                                    )
        self.assertEqual(MessageToDict(self.zone.tags), tags)

    def test_get_zone(self):
        name = 'test-zone'
        self.test_create_zone(name)

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }
        self.zone = self.inventory_v1.Zone.get(param,
                                               metadata=(('token', self.token),)
                                               )
        self.assertEqual(self.zone.name, name)

    def test_add_member_zone(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        zone_admin = self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))
        user_info = MessageToDict(zone_admin.user_info)

        self.assertEqual(user_info.get('user_id'), self.user.user_id)

    def test_add_member_not_exist_user(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': 'test',
            'domain_id': self.domain.domain_id
        }

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

    def test_add_member_duplicate_user(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param,metadata=(('token', self.token),))

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

    def test_add_member_not_exist_zone(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': 'test',
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

    def test_modify_member_zone(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        zone_member = self.inventory_v1.Zone.modify_member(param, metadata=(('token', self.token),))
        user_info = MessageToDict(zone_member.user_info)

        self.assertEqual(user_info.get('user_id'), self.user.user_id)

    def test_modify_member_zone_labels(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        labels = ['developer', 'operator', 'operator']

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id,
            'labels': labels
        }

        zone_member = self.inventory_v1.Zone.modify_member(param, metadata=(('token', self.token),))

        print(zone_member.labels)
        user_info = MessageToDict(zone_member.user_info)

        self.assertEqual(user_info.get('user_id'), self.user.user_id)

    def test_modify_member_not_exist_user(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': 'test',
            'domain_id': self.domain.domain_id,
        }

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.modify_member(param, metadata=(('token', self.token),))

    def test_modify_member_not_exist_zone(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': 'test',
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id,
        }

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.modify_member(param, metadata=(('token', self.token),))

    def test_remove_member_region(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.remove_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        zone_members = self.inventory_v1.Zone.list_members(param, metadata=(('token', self.token),))
        self.assertEqual(0, zone_members.total_count)

    def test_remove_member_not_exist_user(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': 'test',
            'domain_id': self.domain.domain_id
        }

        with self.assertRaises(Exception):
            self.inventory_v1.Zone.remove_member(param, metadata=(('token', self.token),))

    def test_list_members_zone_id(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        zone_members = self.inventory_v1.Zone.list_members(param, metadata=(('token', self.token),))
        self.assertEqual(1, zone_members.total_count)

    def test_list_members_zone_user_id(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        zone_members = self.inventory_v1.Zone.list_members(param, metadata=(('token', self.token),))
        self.assertEqual(1, zone_members.total_count)

    def test_list_members_zone_query(self):
        self.test_create_zone()
        self._create_user()

        param = {
            'zone_id': self.zone.zone_id,
            'user_id': self.user.user_id,
            'domain_id': self.domain.domain_id
        }

        self.inventory_v1.Zone.add_member(param, metadata=(('token', self.token),))

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {'k': 'user_id',
                     'v': self.user.user_id,
                     'o': 'eq'}
                ]
            }
        }

        zone_members = self.inventory_v1.Zone.list_members(param, metadata=(('token', self.token),))
        self.assertEqual(1, zone_members.total_count)

    def test_list_region_id(self):
        self.test_create_zone()
        self.test_create_zone(region_id=self.region.region_id)

        param = {
            'region_id': self.region.region_id,
            'domain_id': self.domain.domain_id
        }

        zones = self.inventory_v1.Zone.list(param, metadata=(('token', self.token),))

        self.assertEqual(2, zones.total_count)

    def test_list_zone_id(self):
        self.test_create_zone()
        self.test_create_zone()

        param = {
            'zone_id': self.zone.zone_id,
            'domain_id': self.domain.domain_id
        }

        zones = self.inventory_v1.Zone.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, zones.total_count)

    def test_list_name(self):
        self.test_create_zone()
        self.test_create_zone()

        param = {
            'name': self.zone.name,
            'domain_id': self.domain.domain_id
        }

        zones = self.inventory_v1.Zone.list(param, metadata=(('token', self.token),))

        self.assertEqual(1, zones.total_count)

    def test_list_query(self):
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'zone_id',
                        'v': list(map(lambda zone: zone.zone_id, self.zones)),
                        'o': 'in'
                    }
                ]
            }
        }

        zones = self.inventory_v1.Zone.list(param, metadata=(('token', self.token),))
        self.assertEqual(len(self.zones), zones.total_count)

    def test_list_query_2(self):
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()
        self.test_create_zone()

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'minimal': True
            }
        }

        zones = self.inventory_v1.Zone.list(param, metadata=(('token', self.token),))

        print(zones.results)

        self.assertEqual(len(self.zones), zones.total_count)

    def test_stat_zones(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': {
                    'group': {
                        'keys': [{
                            'key': 'zone_id',
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

        result = self.inventory_v1.Zone.stat(
            params, metadata=(('token', self.token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

