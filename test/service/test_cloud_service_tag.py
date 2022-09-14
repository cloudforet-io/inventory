import unittest

from mongoengine import connect, disconnect
from spaceone.core import config, utils
from spaceone.core.transaction import Transaction
from spaceone.core.unittest.result import print_data

from spaceone.inventory.model import CloudService
from spaceone.inventory.service import CloudServiceService


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
            'api_class': 'CloudService'
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

    def test_create_cloud_service_contain_dot_tag(self):
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

        self.transaction.method = 'create'
        cloud_svc_service = CloudServiceService(metadata=None)
        cloud_svc_vo = cloud_svc_service.create(params.copy())

        print_data(cloud_svc_vo.to_dict(), 'test_create_cloud_service')

        self.assertIsInstance(cloud_svc_vo, CloudService)
        self.assertEqual(params['cloud_service_type'], cloud_svc_vo.cloud_service_type)

    def test_create_cloud_service_contain_same_nested_key_tag(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            'tags': {'a.b.c': 'd', 'a.b': 'e'},
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),

        }

        self.transaction.method = 'create'
        cloud_svc_service = CloudServiceService(metadata=None)
        with self.assertRaises(Exception):
            cloud_svc_service.create(params.copy())




if __name__ == '__main__':
    unittest.main()
