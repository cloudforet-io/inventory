import os
import unittest
import pprint
from google.protobuf.json_format import MessageToDict

from spaceone.core import utils, pygrpc
from spaceone.core.unittest.runner import RichTestRunner


class TestResourceGroup(unittest.TestCase):
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
        super(TestResourceGroup, cls).setUpClass()
        endpoints = cls.config.get('ENDPOINTS', {})

        cls.identity_v1 = pygrpc.client(endpoint=endpoints.get('identity', {}).get('v1'), version='v1')
        cls.inventory_v1 = pygrpc.client(endpoint=endpoints.get('inventory', {}).get('v1'), version='v1')

        cls._create_domain()
        cls._create_domain_owner()
        cls._issue_owner_token()

    @classmethod
    def tearDownClass(cls):
        super(TestResourceGroup, cls).tearDownClass()
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

    def _create_project_group(self, name=None):
        if name is None:
            name = 'ProjectGroup-' + utils.random_string()

        params = {
            'name': name,
            'domain_id': self.domain.domain_id
        }

        self.project_group = self.identity_v1.ProjectGroup.create(
            params,
            metadata=(('token', self.owner_token),)
        )

        self.project_groups.append(self.project_group)
        self.assertEqual(self.project_group.name, params['name'])

    def _create_project(self, project_group_id, name=None):
        if name is None:
            name = 'Project-' + utils.random_string()

        params = {
            'name': name,
            'project_group_id': project_group_id,
            'domain_id': self.domain.domain_id
        }

        self.project = self.identity_v1.Project.create(
            params,
            metadata=(('token', self.owner_token),)
        )

        self.projects.append(self.project)
        self.assertEqual(self.project.name, params['name'])

    def _print_data(self, message, description=None):
        print()
        if description:
            print(f'[ {description} ]')

        self.pp.pprint(MessageToDict(message, preserving_proto_field_name=True))

    def setUp(self):
        self.resource_groups = []
        self.resource_group = None
        self.projects = []
        self.project = None
        self.project_groups = []
        self.project_group = None

    def tearDown(self):
        for resource_Group in self.resource_groups:
            self.inventory_v1.ResourceGroup.delete(
                {'resource_group_id': resource_Group.resource_group_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )
            print(f'>> delete resource group: {resource_Group.name} ({resource_Group.resource_group_id})')

        for project in self.projects:
            self.identity_v1.Project.delete(
                {'project_id': project.project_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )
            print(f'>> delete project: {project.name} ({project.project_id})')

        for project_group in self.project_groups:
            self.identity_v1.ProjectGroup.delete(
                {'project_group_id': project_group.project_group_id,
                 'domain_id': self.domain.domain_id},
                metadata=(('token', self.owner_token),)
            )
            print(f'>> delete project group: {project_group.name} ({project_group.project_group_id})')

    def test_create_resource_group(self, name=None, project_create=False):
        """ Create Resource Group
        """

        if not name:
            name = utils.random_string()

        params = {
            'name': name,
            'resources': [
                {
                    'resource_type': 'inventory.Server',
                    'filter': [
                        {'k': 'data.compute.aws_tags.Schedule', 'v': 'abcde', 'o': 'eq'},
                        {'k': 'data.compute.aws_tags.Value', 'v': ['bbbbb'], 'o': 'eq'},
                        # {'k': 'data.compute.aws_tags.Key', 'v': 'Policy', 'o': 'eq'},
                        # {'k': 'data.compute.aws_tags.Value', 'v': 'N', 'o': 'eq'}
                    ],
                    'keyword': 'aa bb cc'
                },
                {
                    'resource_type': 'CloudService?provider=aws&cloud_service_group=DynamoDB&cloud_service_type=Table',
                    'filter': [
                        {'k': 'data.compute.aws_tags.Schedule', 'v': 'Test', 'o': 'eq'},
                        {'k': 'data.compute.aws_tags.Value', 'v': 'aaa', 'o': 'eq'},
                        {'k': 'data.compute.aws_tags.Key', 'v': 'Policy', 'o': 'eq'},
                        {'k': 'data.compute.aws_tags.Value', 'v': 'N', 'o': 'eq'}
                    ]
                },
            ],
            'options': {
                'raw_filter': 'aaa.bbb.ccc'
            },
            'domain_id': self.domain.domain_id
        }

        if project_create:
            self._create_project_group()
            self._create_project(self.project_group.project_group_id)

            params.update({
                'project_id': self.project.project_id
            })

        self.resource_group = self.inventory_v1.ResourceGroup.create(
            params,
            metadata=(('token', self.owner_token),))

        self._print_data(self.resource_group, 'test_create_resource_group')

        self.resource_groups.append(self.resource_group)
        self.assertEqual(self.resource_group.name, name)

    def test_update_resource_group_name(self):
        self.test_create_resource_group()

        name = utils.random_string()
        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'name': name,
            'domain_id': self.domain.domain_id,
        }
        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))
        self.assertEqual(self.resource_group.name, name)

    def test_update_resource_group_resource(self):
        self.test_create_resource_group()
        update_resource = [
            {
                'resource_type': 'inventory.Server',
                'filter': [
                    {'k': 'data.compute.xxxx', 'v': 'abcde', 'o': 'eq'},
                ],
                'keyword': 'xx yy zz'
            },
        ]

        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'resources': update_resource,
            'domain_id': self.domain.domain_id,
        }
        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))

        self._print_data(self.resource_group, 'test_update_resource_group_resource')

        self.assertEqual(len(self.resource_group.resources), len(update_resource))

    def test_update_resource_group_project(self):
        self.test_create_resource_group()

        self._create_project_group()
        self._create_project(project_group_id=self.project_group.project_group_id)

        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'project_id': self.project.project_id,
            'domain_id': self.domain.domain_id,
        }

        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(self.resource_group.project_id, self.project.project_id)

    def test_update_resource_group_options(self):
        self.test_create_resource_group()

        options = {
            utils.random_string(): utils.random_string(),
            utils.random_string(): utils.random_string()
        }
        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'options': options,
            'domain_id': self.domain.domain_id,
        }
        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))

        self._print_data(self.resource_group, 'test_update_resource_group_options')

        self.assertEqual(MessageToDict(self.resource_group.options), options)

    def test_update_resource_group_tags(self):
        self.test_create_resource_group()

        tags = {
            utils.random_string(): utils.random_string(),
            utils.random_string(): utils.random_string()
        }
        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'tags': tags,
            'domain_id': self.domain.domain_id
        }
        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))
        resource_group_data = MessageToDict(self.resource_group)
        self.assertEqual(resource_group_data['tags'], tags)

    def test_update_resource_group_release_project(self):
        self.test_create_resource_group(project_create=True)

        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'release_project': True,
            'domain_id': self.domain.domain_id,
        }
        self.resource_group = self.inventory_v1.ResourceGroup.update(
            param,
            metadata=(('token', self.owner_token),))
        self.assertEqual(self.resource_group.project_id, '')

    def test_get_resource_group(self):
        name = 'test-resource_group'
        self.test_create_resource_group(name)

        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'domain_id': self.domain.domain_id
        }
        self.resource_group = self.inventory_v1.ResourceGroup.get(
            param,
            metadata=(('token', self.owner_token),)
        )
        self.assertEqual(self.resource_group.name, name)

    def test_list_resource_group_id(self):
        self.test_create_resource_group(name='test-xxx')
        self.test_create_resource_group(name='test-yyy')

        param = {
            'resource_group_id': self.resource_group.resource_group_id,
            'domain_id': self.domain.domain_id
        }

        resource_groups = self.inventory_v1.ResourceGroup.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(1, resource_groups.total_count)

    def test_list_resource_group_name(self):
        self.test_create_resource_group(name='test-xxx')
        self.test_create_resource_group(name='test-yyy')

        param = {
            'name': 'test-xxx',
            'domain_id': self.domain.domain_id
        }

        resource_groups = self.inventory_v1.ResourceGroup.list(
            param,
            metadata=(('token', self.owner_token),))

        self.assertEqual(1, resource_groups.total_count)

    def test_list_query(self):
        self.test_create_resource_group(name='test-xxx')
        self.test_create_resource_group(name='test-yyy', project_create=True)
        self.test_create_resource_group(name='test-yyy', project_create=True)
        self.test_create_resource_group(name='test-xxx')
        self.test_create_resource_group(name='test-xxx')

        param = {
            'domain_id': self.domain.domain_id,
            'query': {
                'filter': [
                    {
                        'k': 'project_id',
                        'v': list(map(lambda project: project.project_id, self.projects)),
                        'o': 'in'
                    }
                ]
            }
        }

        resource_groups = self.inventory_v1.ResourceGroup.list(
            param,
            metadata=(('token', self.owner_token),))
        self.assertEqual(2, resource_groups.total_count)

    def test_stat_resource_group(self):
        self.test_list_query()

        params = {
            'domain_id': self.domain.domain_id,
            'query': {
                'aggregate': [{
                    'group': {
                        'keys': [{
                            'key': 'resource_group_id',
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

        result = self.inventory_v1.ResourceGroup.stat(
            params,
            metadata=(('token', self.owner_token),))

        print(result)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)

