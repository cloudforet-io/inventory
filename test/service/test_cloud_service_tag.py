import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data

from spaceone.inventory.model import CloudService
from spaceone.inventory.service import CloudServiceService, CloudServiceTagService


class TestCloudServiceTagService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.inventory')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.transaction = Transaction({
            'service': 'inventory',
            'resource': 'CloudService'
        })
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all data_sources')
        cloud_service_vos = CloudService.objects.filter()
        cloud_service_vos.delete()

    def test_create_cloud_service_contain_list_of_dict_tags_by_collector(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            'tags': [
                {'key': 'a.b.c',
                 'value': 'd'},
                {'key': 'b.c.d',
                 'value': 'e'}
            ],
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),

        }

        metadata = {
            'verb': 'create',
            'collector_id': utils.generate_id('collector'),
            'job_id': utils.generate_id('job'),
            'plugin_id': utils.generate_id('plugin'),
            'secret.service_account_id': utils.generate_id('sa'),
        }

        cloud_svc_service = CloudServiceService(metadata=metadata)
        cloud_svc_vo = cloud_svc_service.create(params.copy())

        cloud_svc_tag_service = CloudServiceTagService(metadata=metadata)
        cloud_svc_tag_vos = cloud_svc_tag_service.list({'domain_id': params['domain_id']})

        print_data(cloud_svc_vo.to_dict(), 'test_create_cloud_service')
        for tag in cloud_svc_tag_vos[0]:
            print_data(tag.to_dict(), 'test_cloud_service_tag')

        self.assertIsInstance(cloud_svc_vo, CloudService)
        self.assertEqual(params['cloud_service_type'], cloud_svc_vo.cloud_service_type)
        self.assertEqual(cloud_svc_vo.tags[0].provider, params['provider'])
        self.assertEqual(cloud_svc_vo.tags[0].type, 'PROVIDER')

    def test_create_cloud_service_contain_dot_tag_by_custom(self):
        params = {
            'cloud_service_type': 'Instance',
            'data': {},
            'provider': 'aws',
            'tags': {
                'a.b.c': 'd',
                'a.c.d': 'e',
                'b.c.d.e': 'f'
            },
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),

        }

        cloud_svc_service = CloudServiceService(metadata=None)
        cloud_svc_vo = cloud_svc_service.create(params.copy())

        cloud_svc_tag_service = CloudServiceTagService(metadata=None)
        cloud_svc_tag_vos = cloud_svc_tag_service.list({'domain_id': params['domain_id']})

        print_data(cloud_svc_vo.to_dict(), 'test_create_cloud_service')
        for tag in cloud_svc_tag_vos[0]:
            print_data(tag.to_dict(), 'test_cloud_service_tag')

        self.assertIsInstance(cloud_svc_vo, CloudService)
        self.assertEqual(params['cloud_service_type'], cloud_svc_vo.cloud_service_type)
        self.assertEqual(cloud_svc_vo.tags[0].type, 'CUSTOM')

    def test_create_cloud_service_custom_tags(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            'tags': {
                'a.b.c': 'd',
                'a.c.d': 'e',
                'b.c.d.e': 'f'
            },
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),

        }

        cloud_svc_service = CloudServiceService(metadata=None)
        cloud_svc_vo = cloud_svc_service.create(params.copy())

        cloud_svc_tag_service = CloudServiceTagService(metadata=None)
        cloud_svc_tag_vos = cloud_svc_tag_service.list({'domain_id': params['domain_id']})

        print_data(cloud_svc_vo.to_dict(), 'test_create_cloud_service')
        for tag in cloud_svc_tag_vos[0]:
            print_data(tag.to_dict(), 'test_cloud_service_tag')

        self.assertIsInstance(cloud_svc_vo, CloudService)
        self.assertEqual(params['cloud_service_type'], cloud_svc_vo.cloud_service_type)
        self.assertEqual(cloud_svc_vo.tags[0].type, 'CUSTOM')

    def test_update_cloud_service_all_custom(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            'tags': {
                'a.b.c': 'd',
                'b.c.d': 'e',
                'c.d.e': 'f',
                'a': 'd',
                'a.a': 'e'
            },
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),
        }

        cloud_svc_service = CloudServiceService(metadata=None)
        old_cloud_svc_vo = cloud_svc_service.create(params.copy())
        print_data(old_cloud_svc_vo.to_dict(), 'test_create_cloud_service')

        update_params = {
            'cloud_service_id': old_cloud_svc_vo.cloud_service_id,
            'tags': {
                'a': 'b',
                'a.a': 'c',
                'a.b.c': 'f'
            },
            'domain_id': old_cloud_svc_vo.domain_id
        }
        cloud_svc_service = CloudServiceService(meatadata=None)
        new_cloud_svc_vo = cloud_svc_service.update(update_params)
        print_data(new_cloud_svc_vo.to_dict(), 'test_update_cloud_service')

        cloud_svc_tag_service = CloudServiceTagService(metadata=None)
        cloud_svc_tag_vos = cloud_svc_tag_service.list({'domain_id': params['domain_id']})
        for tag in cloud_svc_tag_vos[0]:
            print_data(tag.to_dict(), 'test_cloud_service_tag')

        self.assertEqual(cloud_svc_tag_vos[1], 3)

    def test_update_cloud_service_mix_provider(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            'tags': {
                'a.b.c': 'd',
                'b.c.d': 'e',
                'c.d.e': 'f',
            },
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),
        }

        metadata = {
            'verb': 'create',
            'collector_id': utils.generate_id('collector'),
            'job_id': utils.generate_id('job'),
            'plugin_id': utils.generate_id('plugin'),
            'secret.service_account_id': utils.generate_id('sa'),
        }

        cloud_svc_service = CloudServiceService(metadata=metadata)
        old_cloud_svc_vo = cloud_svc_service.create(params.copy())

        update_params = {
            'cloud_service_id': old_cloud_svc_vo.cloud_service_id,
            'provider': 'aws',
            'tags': {
                'a': 'b',
                'a.a': 'c',
                'a.b.c': 'f'
            },
            'domain_id': old_cloud_svc_vo.domain_id
        }

        cloud_svc_service = CloudServiceService(metadta=None)
        new_cloud_svc_vo = cloud_svc_service.update(update_params)
        print_data(new_cloud_svc_vo.to_dict(), 'test_update_cloud_service')

        cloud_svc_tag_service = CloudServiceTagService(metadata=None)
        cloud_svc_tag_vos = cloud_svc_tag_service.list({'domain_id': params['domain_id']})
        for tag in cloud_svc_tag_vos[0]:
            print_data(tag.to_dict(), 'test_cloud_service_tag')

        self.assertEqual(cloud_svc_tag_vos[1], 6)


if __name__ == '__main__':
    unittest.main()
