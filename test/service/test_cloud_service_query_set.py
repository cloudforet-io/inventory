import unittest
from unittest.mock import patch
import mongomock
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils

from spaceone.inventory.error import *
from spaceone.inventory.service.cloud_service_query_set_service import CloudServiceQuerySetService
from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet
from spaceone.inventory.info.cloud_service_query_set_info import CloudServiceQuerySetInfo
from test.factory.cloud_service_query_set_factory import CloudServiceQuerySetFactory


class TestCloudServiceQuerySet(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.inventory')
        config.set_service_config()
        config.set_global(MOCK_MODE=True)
        connect('test', host='mongodb://localhost', mongo_client_class=mongomock.MongoClient)

        cls.domain_id = utils.generate_id('domain')
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete all cloud service query sets')
        query_set_vos = CloudServiceQuerySet.objects.filter()
        query_set_vos.delete()

    def test_create_cloud_service_query_set(self, *args):
        params = {
            'name': 'EC2 Count by Instance Type',
            'query_options': {
                'group_by': ['instance_type'],
                'fields': {
                    'instance_count': {
                        'operator': 'count',
                    }
                }
            },
            'unit': {
                'instance_count': 'Count'
            },
            'provider': 'aws',
            'cloud_service_group': 'EC2',
            'cloud_service_type': 'Instance',
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'create'})

        query_set_vo: CloudServiceQuerySet = cloud_service_query_set_svc.create(params.copy())
        print_data(query_set_vo.to_dict(), 'test_create_cloud_service_query_set')

        query_set_data = dict(query_set_vo.to_dict())

        self.assertEqual(params['name'], query_set_data['name'])
        self.assertEqual(params['query_options'], query_set_data['query_options'])
        self.assertEqual('CUSTOM', query_set_data['query_type'])
        self.assertEqual(params.get('unit', {}), query_set_data['unit'])
        self.assertEqual(params['provider'], query_set_data['provider'])
        self.assertEqual(params['cloud_service_group'], query_set_data['cloud_service_group'])
        self.assertEqual(params['cloud_service_type'], query_set_data['cloud_service_type'])
        self.assertEqual(params['tags'], query_set_data['tags'])
        self.assertEqual(params['domain_id'], query_set_data['domain_id'])

        CloudServiceQuerySetInfo(query_set_vo)

    def test_update_cloud_service_query_set(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='CUSTOM')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'name': 'EC2 Size by Instance Type',
            'query_options': {
                'group_by': ['instance_type'],
                'fields': {
                    'instance_size': {
                        'key': 'instance_size',
                        'operator': 'sum',
                    }
                }
            },
            'tags': {
                'update_key': 'update_value'
            },
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'update'})

        query_set_vo: CloudServiceQuerySet = cloud_service_query_set_svc.update(params.copy())
        print_data(query_set_vo.to_dict(), 'test_update_cloud_service_query_set')

        query_set_data = dict(query_set_vo.to_dict())
        self.assertEqual(params['name'], query_set_data['name'])
        self.assertEqual(params['query_options'], query_set_data['query_options'])
        self.assertEqual(params['tags'], query_set_data['tags'])

        CloudServiceQuerySetInfo(query_set_vo)

    def test_update_managed_query_type(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='MANAGED')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'update'})

        with self.assertRaises(ERROR_NOT_ALLOWED_QUERY_TYPE):
            cloud_service_query_set_svc.update(params.copy())

    def test_delete_cloud_service_query_set(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='CUSTOM')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'delete'})

        cloud_service_query_set_svc.delete(params)

    def test_enable_cloud_service_query_set(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='CUSTOM',
                                                                             state='DISABLED')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'enable'})

        query_set_vo: CloudServiceQuerySet = cloud_service_query_set_svc.enable(params.copy())
        print_data(query_set_vo.to_dict(), 'test_enable_cloud_service_query_set')

        query_set_data = dict(query_set_vo.to_dict())
        self.assertEqual('ENABLED', query_set_data['state'])

        CloudServiceQuerySetInfo(query_set_vo)

    def test_disable_cloud_service_query_set(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='CUSTOM',
                                                                             state='ENABLED')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'disable'})

        query_set_vo: CloudServiceQuerySet = cloud_service_query_set_svc.disable(params.copy())
        print_data(query_set_vo.to_dict(), 'test_disable_cloud_service_query_set')

        query_set_data = dict(query_set_vo.to_dict())
        self.assertEqual('DISABLED', query_set_data['state'])

        CloudServiceQuerySetInfo(query_set_vo)

    def test_get_cloud_service_query_set(self, *args):
        new_query_set_vo: CloudServiceQuerySet = CloudServiceQuerySetFactory(domain_id=self.domain_id,
                                                                             query_type='CUSTOM')

        params = {
            'query_set_id': new_query_set_vo.query_set_id,
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'get'})

        query_set_vo: CloudServiceQuerySet = cloud_service_query_set_svc.get(params)
        print_data(query_set_vo.to_dict(), 'test_get_cloud_service_query_set')

        CloudServiceQuerySetInfo(query_set_vo)

    def test_list_cloud_service_query_sets_by_name(self, *args):
        query_set_vos = CloudServiceQuerySetFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), query_set_vos))

        params = {
            'name': query_set_vos[0]['name'],
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'list'})

        query_sets_vo, total_count = cloud_service_query_set_svc.list(params)
        print_data(query_sets_vo, 'test_list_cloud_service_query_sets_by_name')

        self.assertEqual(len(query_sets_vo), 1)
        self.assertIsInstance(query_sets_vo[0], CloudServiceQuerySet)
        self.assertEqual(total_count, 1)

    def test_list_cloud_service_query_sets_by_tags(self, *args):
        CloudServiceQuerySetFactory(tags={'tag_key_1': 'tag_value_1'}, domain_id=self.domain_id)
        query_set_vos = CloudServiceQuerySetFactory.build_batch(9, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), query_set_vos))

        params = {
            'query': {
                'filter': [{
                    'k': 'tags.tag_key_1',
                    'v': 'tag_value_1',
                    'o': 'eq'
                }]
            },
            'domain_id': self.domain_id
        }

        cloud_service_query_set_svc = CloudServiceQuerySetService(
            metadata={'resource': 'CloudServiceQuerySet', 'verb': 'list'})

        query_sets_vo, total_count = cloud_service_query_set_svc.list(params)
        print_data(query_sets_vo, 'test_list_cloud_service_query_sets_by_tags')

        self.assertEqual(len(query_sets_vo), 1)
        self.assertIsInstance(query_sets_vo[0], CloudServiceQuerySet)
        self.assertEqual(total_count, 1)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
