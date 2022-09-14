import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect
from spaceone.core import config, utils, pygrpc
from spaceone.core.transaction import Transaction
from spaceone.core.model.mongo_model import MongoModel
from spaceone.core.unittest.result import print_data

from spaceone.inventory.model import CloudService
from spaceone.inventory.service import CloudServiceService

from test.factory.cloud_service_factory import CloudServiceFactory


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

    def test_create_cloud_service_contain_tags(self):
        params = {
            'cloud_service_type': 'Instance',
            'provider': 'aws',
            'data': {},
            # 'tags': {'a.b.c': 'd'},
            "region_code": "ap-northeast-2",
            'cloud_service_group': 'EC2',
            'domain_id': utils.generate_id('domain'),

        }

        self.transaction.method = 'create'
        cloud_svc_service = CloudServiceService(metadata=None)
        cloud_svc_vo = cloud_svc_service.create(params.copy())

        print_data(cloud_svc_vo.to_dict(), 'test_create_cloud_service')
